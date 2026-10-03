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
use crate::geometry::distance::{ChartEdgePart, ChartParts, point_chart_locate};
use crate::geometry::{
    ChartMetric, CoordSeq, Coordinates as _, GeodesicMetric, GeometryErrorKind, LineSeq,
    MeasureRange, Ordering, Point, Segment, Shape, ShapeCountQuery, lerp_point,
    linework_first_point, normalize_line_distance, point_distance,
    point_segment_distance_and_projection, push_distinct_active_position, same_point,
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
    let point = metric.chart_edge_witness_with_length(start, end, edge, parameter, length)?;
    if provenance == InterpolationProvenance::Independent {
        return Ok(point);
    }
    let lifted_position = edge.chart_position(parameter);
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

macro_rules! locate_point_body {
    (
        $this:expr,
        $probe:expr,
        $normalized:expr,
        $metric:expr,
        $metric_type:ty,
        $index:ident,
        $segment:ident,
        $offset:ident,
        $length:ident,
        $skip_zero_segments:block
    ) => {{
        let mut best_distance = f64::INFINITY;
        let mut best_offset = 0.0;
        {
            let mut best_projection = None;
            for ($index, ($segment, &$offset)) in
                std::iter::zip(&($this).segments, &($this).prefix).enumerate()
            {
                let $length = ($this).prefix[$index + 1] - $offset;
                $skip_zero_segments;
                let gap = (($probe).x - ($segment).start.x.max(($segment).end.x))
                    .max(($segment).start.x.min(($segment).end.x) - ($probe).x)
                    .max(($probe).y - ($segment).start.y.max(($segment).end.y))
                    .max(($segment).start.y.min(($segment).end.y) - ($probe).y);
                // Axis separation bounds Euclidean distance from below. Leave
                // a factor-two margin against the measurement's rounding;
                // overflow and underflow stay on the existing metric path.
                if gap.is_normal() && gap > best_distance * 2.0 {
                    continue;
                }
                let (distance, projection) =
                    point_segment_distance_and_projection(($probe).xy(), Segment {
                        start: ($segment).start.into(),
                        end: ($segment).end.into(),
                    })?;
                if distance < best_distance {
                    best_distance = distance;
                    best_projection = Some((projection, $offset, $length));
                }
            }
            if let Some((projection, offset, length)) = best_projection {
                best_offset = offset + projection.scale(length)?;
            }
        }
        if $normalized {
            Ok(best_offset / ($this).total)
        } else {
            Ok(best_offset)
        }
    }};
}

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
    /// behind `line_locate_point`). First-wins tie ordering, like the scan.
    #[expect(
        clippy::indexing_slicing,
        reason = "locate indices stay paired with their segment and trailing prefix entry"
    )]
    pub(crate) fn locate_point<M: SegmentMetric>(
        &self,
        probe: Point,
        normalized: bool,
        metric: &M,
    ) -> Result<f64> {
        // POINT EMPTY reaches a locate as the NaN point of a point column.
        if probe.x.is_nan() {
            return Ok(f64::NAN);
        }
        if self.total == 0.0 {
            return Ok(0.0);
        }
        if self.has_zero_segments {
            self.locate_point_with_zero_segments(probe, normalized, metric)
        } else {
            locate_point_body!(
                self,
                probe,
                normalized,
                metric,
                M,
                index,
                segment,
                offset,
                length,
                {}
            )
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "locate indices stay paired with their segment and trailing prefix entry"
    )]
    fn locate_point_with_zero_segments<M: SegmentMetric>(
        &self,
        probe: Point,
        normalized: bool,
        metric: &M,
    ) -> Result<f64> {
        locate_point_body!(
            self,
            probe,
            normalized,
            metric,
            M,
            index,
            segment,
            offset,
            length,
            {
                if length == 0.0
                    && (same_point(segment.start, segment.end)
                        || metric.admitted_length(segment.start, segment.end)? == 0.0)
                {
                    continue;
                }
            }
        )
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
        let index = LineIndex::build(&line, &PlanarMetric).unwrap();

        assert_eq!(
            index.locate_point(points[2], true, &PlanarMetric).unwrap(),
            1.0
        );
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
