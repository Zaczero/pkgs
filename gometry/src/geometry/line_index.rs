//! Prefix-length linear-referencing engine: one build walks the linework
//! and caches per-segment lengths + cumulative offsets, then interpolate /
//! substring / locate run off the prefix table instead of re-measuring every
//! segment per call. Parameterized by a [`SegmentMetric`] so the planar and
//! geodesic engines share one lifecycle — for the geodesic metric the build
//! pays the per-segment Karney inverse exactly once per (shape, CRS).
//!
//! Zero-length segments stay in the route sequence at their unchanged prefix
//! and contribute their stored vertices to interpolation and substrings.
//! Locate skips them, matching the former projection walk; exact prefix
//! boundaries resolve to the first vertex at that prefix, except the end,
//! which resolves to the last stored vertex.

use crate::HeapSize;
use crate::error::Result;
use crate::geometry::{
    CoordSeq, Coordinates as _, GeodesicMetric, GeodesicSegmentQuery, GeodesicSegmentQueryResult,
    GeodesicSegmentScratch, GeodesicWitnessNeed, GeometryErrorKind, LineSeq, MeasureRange,
    Ordering, Point, Segment, Shape, ShapeCountQuery, interpolate_f64, lerp_point,
    linework_first_point, normalize_line_distance, point_distance,
    point_segment_distance_and_projection, push_distinct_active_position, same_point,
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum LocateStrategy {
    Planar,
    Ordered,
}

/// Per-segment measurement for the linear-referencing engine.
pub(crate) trait SegmentMetric {
    /// Length of the segment `start`–`end` in this metric.
    fn length(&self, start: Point, end: Point) -> f64;
    /// Admit the segment and return its length. Geodesic metrics use this
    /// boundary to reject non-unique shortest arcs before an index is cached.
    fn admitted_length(&self, start: Point, end: Point) -> Result<f64> {
        Ok(self.length(start, end))
    }
    /// The point at `fraction` (`[0, 1]`) along the segment.
    fn interpolate(&self, start: Point, end: Point, fraction: f64) -> Point;
    /// Candidate ordering and projection kernel selected by this metric.
    const LOCATE_STRATEGY: LocateStrategy;
    /// Whether `locate_with_measure` needs the segment length argument.
    /// Planar M interpolation keeps the certified projection ratio and does
    /// not use the length; geodesic refinement needs its along-track scale.
    const LOCATE_WITH_MEASURE_NEEDS_LENGTH: bool = true;

    /// Cheap lower bound on this segment's locate distance (`0.0` when the
    /// metric has none). Sound: never exceeds the true distance; preparation
    /// failures are returned instead of disabling the bound.
    fn locate_bound(&self, _probe: Point, _start: Point, _end: Point, _length: f64) -> Result<f64> {
        Ok(0.0)
    }

    /// `(distance to probe, along-segment offset of the projection)`, or
    /// `None` when the segment is excluded by the running bound.
    /// `length` is the segment's cached metric length and `best` the running
    /// minimum distance. Metrics with expensive refinement (geodesic)
    /// may return `None` once a cheap bound proves the segment cannot improve
    /// on it.
    fn locate(
        &self,
        probe: Point,
        start: Point,
        end: Point,
        length: f64,
        best: f64,
        scratch: &mut GeodesicSegmentScratch,
    ) -> Result<Option<(f64, f64)>>;

    /// Locate a probe and return the interpolated M value at its foot. Planar
    /// metrics override this to retain certified projection ratios that may be
    /// too small to survive conversion through an along-distance.
    fn locate_with_measure(
        &self,
        probe: Point,
        start: Point,
        end: Point,
        length: f64,
        best: f64,
        scratch: &mut GeodesicSegmentScratch,
    ) -> Result<Option<(f64, f64)>> {
        let Some((distance, along)) = self.locate(probe, start, end, length, best, scratch)? else {
            return Ok(None);
        };
        let fraction = if length == 0.0 {
            0.0
        } else {
            (along / length).clamp(0.0, 1.0)
        };
        Ok(Some((
            distance,
            interpolate_f64(start.m().unwrap_or(0.0), end.m().unwrap_or(0.0), fraction),
        )))
    }
}

macro_rules! locate_planar_segment {
    ($probe:expr, $start:expr, $end:expr, $length:expr) => {{
        let (distance, projection) =
            point_segment_distance_and_projection(($probe).xy(), Segment {
                start: ($start).into(),
                end: ($end).into(),
            })?;
        Ok::<Option<(f64, f64)>, crate::error::Error>(Some((distance, projection.scale($length)?)))
    }};
}

/// Planar (coordinate-unit) metric.
pub(crate) struct PlanarMetric;

impl SegmentMetric for PlanarMetric {
    const LOCATE_STRATEGY: LocateStrategy = LocateStrategy::Planar;
    const LOCATE_WITH_MEASURE_NEEDS_LENGTH: bool = false;

    fn length(&self, start: Point, end: Point) -> f64 {
        // The guarded sqrt form: a libm `hypot` per segment is the dominant
        // cost of the cold prefix build (38% of `line_interpolate_point` in
        // profile), and lengths are measurement arithmetic.
        point_distance(start, end)
    }

    fn interpolate(&self, start: Point, end: Point, fraction: f64) -> Point {
        lerp_point(start, end, fraction)
    }

    fn locate(
        &self,
        probe: Point,
        start: Point,
        end: Point,
        length: f64,
        _best: f64,
        _scratch: &mut GeodesicSegmentScratch,
    ) -> Result<Option<(f64, f64)>> {
        locate_planar_segment!(probe, start, end, length)
    }

    fn locate_with_measure(
        &self,
        probe: Point,
        start: Point,
        end: Point,
        _length: f64,
        _best: f64,
        _scratch: &mut GeodesicSegmentScratch,
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

/// Any [`GeodesicMetric`] is a [`SegmentMetric`] (lengths in meters).
impl<M: GeodesicMetric> SegmentMetric for M {
    const LOCATE_STRATEGY: LocateStrategy = LocateStrategy::Ordered;

    fn admitted_length(&self, start: Point, end: Point) -> Result<f64> {
        Ok(self.make_segment(start, end)?.length)
    }

    fn locate_bound(&self, probe: Point, start: Point, end: Point, length: f64) -> Result<f64> {
        // Triangle inequality off the auxiliary-sphere endpoint bounds: the
        // foot is at least the farther-bounded endpoint minus the segment.
        Ok((self
            .point_distance_lower_bound(probe, start)?
            .max(self.point_distance_lower_bound(probe, end)?)
            - length)
            .max(0.0))
    }

    fn length(&self, start: Point, end: Point) -> f64 {
        self.segment_length(start, end)
    }

    fn interpolate(&self, start: Point, end: Point, fraction: f64) -> Point {
        GeodesicMetric::interpolate(self, start, end, fraction)
    }

    fn locate(
        &self,
        probe: Point,
        start: Point,
        end: Point,
        _length: f64,
        best: f64,
        scratch: &mut GeodesicSegmentScratch,
    ) -> Result<Option<(f64, f64)>> {
        let result = self.point_segment_query(
            probe,
            &self.make_segment(start, end)?,
            GeodesicSegmentQuery::Minimum {
                cutoff: best,
                witness: GeodesicWitnessNeed::No,
            },
            scratch,
        )?;
        match result {
            GeodesicSegmentQueryResult::Attained { minimum, .. } => {
                Ok(Some((minimum.distance, minimum.along)))
            },
            GeodesicSegmentQueryResult::Excluded => Ok(None),
            GeodesicSegmentQueryResult::ThresholdOutside
            | GeodesicSegmentQueryResult::ThresholdResolvedOutside { .. } => {
                unreachable!("minimum query returned a threshold outcome")
            },
        }
    }
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
struct OrdinateSegment {
    start: Point,
    end: Point,
}

crate::heapless!(OrdinateSegment);

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
        let mut segment_scratch = GeodesicSegmentScratch::default();
        if <$metric_type as SegmentMetric>::LOCATE_STRATEGY == LocateStrategy::Ordered {
            #[derive(Clone, Copy)]
            struct BoundEntry {
                bound: f64,
                index: usize,
            }
            impl PartialEq for BoundEntry {
                fn eq(&self, other: &Self) -> bool {
                    self.bound.to_bits() == other.bound.to_bits() && self.index == other.index
                }
            }
            impl Eq for BoundEntry {}
            impl PartialOrd for BoundEntry {
                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                    Some(self.cmp(other))
                }
            }
            impl Ord for BoundEntry {
                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                    other
                        .bound
                        .total_cmp(&self.bound)
                        .then_with(|| other.index.cmp(&self.index))
                }
            }
            let mut entries = Vec::with_capacity(($this).segments.len());
            for ($index, ($segment, &$offset)) in
                std::iter::zip(&($this).segments, &($this).prefix).enumerate()
            {
                let $length = ($this).prefix[$index + 1] - $offset;
                $skip_zero_segments;
                let bound =
                    ($metric).locate_bound($probe, $segment.start, $segment.end, $length)?;
                entries.push(BoundEntry {
                    bound,
                    index: $index,
                });
            }
            let mut order = std::collections::BinaryHeap::from(entries);
            let mut best_index = usize::MAX;
            while let Some(BoundEntry { bound, index }) = order.pop() {
                if bound > best_distance {
                    break;
                }
                #[expect(clippy::float_cmp, reason = "exact tie against the refined best")]
                let skippable_tie = bound == best_distance && index > best_index;
                if skippable_tie {
                    continue;
                }
                let segment = ($this).segments[index];
                let offset = ($this).prefix[index];
                let length = ($this).prefix[index.wrapping_add(1)] - offset;
                let Some((distance, along)) = ($metric).locate(
                    $probe,
                    segment.start,
                    segment.end,
                    length,
                    best_distance,
                    &mut segment_scratch,
                )?
                else {
                    continue;
                };
                #[expect(clippy::float_cmp, reason = "tie-break preserves first-wins order")]
                let tie = distance == best_distance && index < best_index;
                if distance < best_distance || tie {
                    best_distance = distance;
                    best_offset = offset + along;
                    best_index = index;
                }
            }
        } else {
            for ($index, ($segment, &$offset)) in
                std::iter::zip(&($this).segments, &($this).prefix).enumerate()
            {
                let $length = ($this).prefix[$index + 1] - $offset;
                $skip_zero_segments;
                let Some((distance, along)) =
                    locate_planar_segment!($probe, $segment.start, $segment.end, $length)?
                else {
                    continue;
                };
                if distance < best_distance {
                    best_distance = distance;
                    best_offset = $offset + along;
                }
            }
        }
        if $normalized {
            Ok(best_offset / ($this).total)
        } else {
            Ok(best_offset)
        }
    }};
}

pub(crate) struct LineIndex {
    /// All stored segments, in linework order. Zero-length segments share a
    /// prefix with the surrounding route vertices.
    segments: Box<[OrdinateSegment]>,
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
                // planar) — N11 closed under free-or-nearly-free. Geodesic
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

    /// The segment containing `target` (a clamped `[0, total]` measure) and
    /// the local offset into it. An exact boundary resolves to the segment it
    /// ends, matching the old `target <= length` walk.
    fn segment_at(&self, target: f64) -> Option<(usize, f64)> {
        if self.segments.is_empty() {
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
    ) -> Point {
        if self.total == 0.0 {
            return self.first;
        }
        let target = normalize_line_distance(distance, self.total, normalized);
        let Some((index, local)) = self.segment_at(target) else {
            return self.last;
        };
        #[expect(
            clippy::indexing_slicing,
            reason = "segment_at returns an index in the segment slice"
        )]
        let segment = self.segments[index];
        #[expect(
            clippy::indexing_slicing,
            reason = "segment_at returns an index with a following prefix entry"
        )]
        let length = self.prefix[index.wrapping_add(1)] - self.prefix[index];
        if local == 0.0 {
            return segment.start;
        }
        #[expect(clippy::float_cmp, reason = "exact prefix boundary identity")]
        if local == length {
            return segment.end;
        }
        metric.interpolate(segment.start, segment.end, local / length)
    }

    pub(crate) fn interpolate_route_position(
        &self,
        distance: f64,
        normalized: bool,
        metric: &impl SegmentMetric,
    ) -> Point {
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
    ) -> Point {
        if self.total == 0.0 {
            return self.interpolate_zero_total(distance, normalized);
        }
        let target = normalize_line_distance(distance, self.total, normalized);
        if target.total_cmp(&self.total) == Ordering::Equal {
            return self.last;
        }
        let Some((index, local)) = self.segment_at(target) else {
            return self.last;
        };
        #[expect(
            clippy::indexing_slicing,
            reason = "segment_at returns an index in the segment slice"
        )]
        let segment = self.segments[index];
        #[expect(
            clippy::indexing_slicing,
            reason = "segment_at returns an index with a following prefix entry"
        )]
        let length = self.prefix[index.wrapping_add(1)] - self.prefix[index];
        if local == 0.0 {
            return segment.start;
        }
        #[expect(clippy::float_cmp, reason = "exact prefix boundary identity")]
        if local == length {
            return segment.end;
        }
        metric.interpolate(segment.start, segment.end, local / length)
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
                self.interpolate_route_position(start, false, metric),
            ));
        }

        let mut result = Vec::new();
        // Only the overlapping prefix window is walked; semantics (distinct
        // consecutive points, segment-end offsets) match the full scan.
        let from_index = self.prefix.partition_point(|&boundary| boundary < start);
        for (segment, prefix_pair) in self
            .segments
            .iter()
            .zip(self.prefix.array_windows::<2>())
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
            // full segments, so this skips ~all the geodesic directs). The
            // comparisons are exact by design: `from`/`to` are clamped to
            // the same prefix values they compare against.
            #[expect(clippy::float_cmp, reason = "exact prefix boundary identity")]
            let at = |target: f64| {
                if target == segment_start {
                    segment.start
                } else if target == segment_end {
                    segment.end
                } else {
                    metric.interpolate(
                        segment.start,
                        segment.end,
                        (target - segment_start) / length,
                    )
                }
            };
            push_distinct_active_position(&mut result, at(from));
            push_distinct_active_position(&mut result, at(to));
        }

        Ok(match result.as_slice() {
            [] => Shape::Point(self.interpolate_route_position(start, false, metric)),
            [point] => Shape::Point(*point),
            _ => Shape::LineString(LineSeq::from_proven(CoordSeq::try_from_points(&result)?)),
        })
    }

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
                        || metric.length(segment.start, segment.end) == 0.0)
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

impl HeapSize for LineIndex {
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
}
