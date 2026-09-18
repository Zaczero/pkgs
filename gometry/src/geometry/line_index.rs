//! Prefix-length linear-referencing engine: one build walks the linework
//! and caches per-segment lengths + cumulative offsets, then interpolate /
//! substring / locate run off the prefix table instead of re-measuring every
//! segment per call. Parameterized by a [`SegmentMetric`] so the planar and
//! geodesic engines share one lifecycle — for the geodesic metric the build
//! pays the per-segment Karney inverse exactly once per (shape, CRS).
//!
//! Semantics are identical to the previous per-call walks: zero-length
//! segments are skipped, an exact prefix boundary resolves to the segment it
//! ends (`target <= length` in the old loop), distances normalize through
//! [`normalize_line_distance`], and locate keeps first-wins tie ordering.

use crate::HeapSize;
use crate::error::Result;
use crate::geometry::{
    CoordSeq, Coordinates as _, GeodesicMetric, GeodesicSegmentQuery, GeodesicSegmentQueryResult,
    GeodesicSegmentScratch, GeodesicWitnessNeed, GeometryErrorKind, LineSeq, MeasureRange,
    Ordering, Point, Segment, Shape, interpolate_f64, lerp_point, linework_first_point,
    normalize_line_distance, point_distance, point_segment_distance_and_projection,
};

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
    /// Whether locate sweeps should refine segments in cheap-lower-bound
    /// order with early termination (worth it only when refinement is
    /// expensive because it evaluates the certified geodesic segment kernel;
    /// planar projection is cheaper
    /// than the ordering would be).
    const ORDERED_LOCATE: bool = false;
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

/// Planar (coordinate-unit) metric.
pub(crate) struct PlanarMetric;

impl SegmentMetric for PlanarMetric {
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
        let (distance, projection) = point_segment_distance_and_projection(probe.xy(), Segment {
            start: start.into(),
            end: end.into(),
        })?;
        Ok(Some((distance, projection.scale(length)?)))
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
    const ORDERED_LOCATE: bool = true;

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
                _ => Ok(Self::Invalid(crate::boundary::try_format(format_args!(
                    "{error}"
                ))?)),
            },
        }
    }

    pub(crate) fn get(&self) -> Result<&LineIndex> {
        match self {
            Self::Index(index) => Ok(index),
            Self::NotLineal => Err(GeometryErrorKind::LineStringRequired.into()),
            Self::Empty => Err(GeometryErrorKind::EmptyLinework.into()),
            Self::Invalid(msg) => Err(GeometryErrorKind::message(crate::boundary::try_boxed_str(
                msg,
            )?)),
        }
    }
}

/// The prefix-length index over one shape's linework, in one metric.
/// One linear-referencing segment with FULL ordinate carry — the planar
/// [`Segment`] is XY-only by design, and LRS interpolates Z/M.
#[derive(Clone, Copy)]
struct OrdinateSegment {
    start: Point,
    end: Point,
}

crate::heapless!(OrdinateSegment);

pub(crate) struct LineIndex {
    /// Non-zero-length segments, in linework order.
    segments: Box<[OrdinateSegment]>,
    /// `prefix[i]` = cumulative length before segment `i`; the last entry is
    /// the cached total length. Lengths are cached here — locate's
    /// `mul_add(length, fraction, offset)` and every interpolation walk read
    /// them back instead of re-measuring.
    prefix: Box<[f64]>,
    total: f64,
    first: Point,
    last: Point,
}

impl LineIndex {
    fn try_storage(upper: usize) -> Result<(Vec<OrdinateSegment>, Vec<f64>)> {
        let prefix_len = upper
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut segments = Vec::new();
        let mut prefix = Vec::new();
        if let Err(error) = crate::boundary::try_reserve_vec(&mut segments, upper) {
            drop(prefix);
            return Err(error);
        }
        if let Err(error) = crate::boundary::try_reserve_vec(&mut prefix, prefix_len) {
            drop(segments);
            return Err(error);
        }
        Ok((segments, prefix))
    }

    /// Build over `shape`'s linework. `LineStringRequired` for non-lineal
    /// shapes, `EmptyLinework` when there is no first vertex (the zero-total
    /// degeneracies — all-zero-length linework — build fine and resolve to
    /// `first`, matching the walks).
    pub(crate) fn build(shape: &Shape, metric: &impl SegmentMetric) -> Result<Self> {
        let lines = shape.linework()?;
        let first = linework_first_point(&lines).ok_or(GeometryErrorKind::EmptyLinework)?;
        let upper = shape.segment_count();
        let (mut segments, mut prefix) = Self::try_storage(upper)?;
        crate::boundary::try_push_vec(&mut prefix, 0.0)?;
        let mut total = 0.0;
        let mut last = first;
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
                crate::boundary::try_push_vec(&mut segments, OrdinateSegment { start, end })?;
                crate::boundary::try_push_vec(&mut prefix, total)?;
            }
        }
        // The walks fall back to the linework's last vertex (zero-length tail
        // segments included), not the last measured segment's end.
        let last = lines
            .iter()
            .rev()
            .find_map(|line| line.last())
            .unwrap_or(last);
        let segments = try_boxed_slice(segments)?;
        let prefix = match try_boxed_slice(prefix) {
            Ok(prefix) => prefix,
            Err(error) => {
                drop(segments);
                return Err(error);
            },
        };
        Ok(Self {
            segments,
            prefix,
            total,
            first,
            last,
        })
    }

    /// Build over one line's zero-copy [`CoordSeq`] window — the packed-`Lines`
    /// batch lane reads CSR rows through this with no per-row `Shape` / `Arc`
    /// traffic (the measured gap vs the `iter_rows` + transient fallback).
    pub(crate) fn build_coordseq(coords: &CoordSeq, metric: &impl SegmentMetric) -> Result<Self> {
        let first = coords.first().ok_or(GeometryErrorKind::EmptyLinework)?;
        let upper = coords.len().saturating_sub(1);
        let (mut segments, mut prefix) = Self::try_storage(upper)?;
        crate::boundary::try_push_vec(&mut prefix, 0.0)?;
        let mut total = 0.0;
        let mut last = first;
        for [start, end] in coords.segment_pairs() {
            last = end;
            let length = metric.admitted_length(start, end)?;
            if !length.is_finite() {
                return Err(GeometryErrorKind::message(
                    "line index cannot include a non-finite segment length",
                ));
            }
            if length == 0.0 {
                continue;
            }
            total += length;
            if !total.is_finite() {
                return Err(GeometryErrorKind::message(
                    "line index total length must be finite",
                ));
            }
            crate::boundary::try_push_vec(&mut segments, OrdinateSegment { start, end })?;
            crate::boundary::try_push_vec(&mut prefix, total)?;
        }
        let last = coords.last().unwrap_or(last);
        let segments = try_boxed_slice(segments)?;
        let prefix = match try_boxed_slice(prefix) {
            Ok(prefix) => prefix,
            Err(error) => {
                drop(segments);
                return Err(error);
            },
        };
        Ok(Self {
            segments,
            prefix,
            total,
            first,
            last,
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
            .min(self.segments.len() - 1);
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
        let segment = self.segments[index];
        let length = self.prefix[index + 1] - self.prefix[index];
        if local == 0.0 {
            return segment.start;
        }
        #[expect(clippy::float_cmp, reason = "exact prefix boundary identity")]
        if local == length {
            return segment.end;
        }
        metric.interpolate(segment.start, segment.end, local / length)
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
        if self.total == 0.0 {
            return Ok(Shape::Point(self.first));
        }
        let (start_distance, end_distance) = (range.start(), range.end());
        let start = normalize_line_distance(start_distance, self.total, normalized);
        let end = normalize_line_distance(end_distance, self.total, normalized);
        if start > end {
            return Err(GeometryErrorKind::SubstringOrder(start_distance, end_distance).into());
        }
        if start.total_cmp(&end) == Ordering::Equal {
            return Ok(Shape::Point(self.interpolate(start, false, metric)));
        }

        let mut result = Vec::new();
        // Only the overlapping prefix window is walked; semantics (distinct
        // consecutive points, segment-end offsets) match the full scan.
        let from_index = self.prefix.partition_point(|&boundary| boundary < start);
        for index in from_index.saturating_sub(1)..self.segments.len() {
            let segment_start = self.prefix[index];
            let segment_end = self.prefix[index + 1];
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
            let segment = self.segments[index];
            let length = segment_end - segment_start;
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
            try_push_distinct_point(&mut result, at(from))?;
            try_push_distinct_point(&mut result, at(to))?;
        }

        Ok(match result.len() {
            0 => Shape::Point(self.interpolate(start, false, metric)),
            1 => Shape::Point(result[0]),
            _ => Shape::LineString(LineSeq::from_proven(CoordSeq::try_from_points(&result)?)),
        })
    }

    /// The along-line offset of `probe`'s nearest projection (the engine
    /// behind `line_locate_point`). First-wins tie ordering, like the scan.
    pub(crate) fn locate_point<M: SegmentMetric>(
        &self,
        probe: Point,
        normalized: bool,
        metric: &M,
    ) -> Result<f64> {
        if self.total == 0.0 {
            return Ok(0.0);
        }
        let mut best_distance = f64::INFINITY;
        let mut best_offset = 0.0;
        let mut segment_scratch = GeodesicSegmentScratch::default();
        if M::ORDERED_LOCATE {
            // Refine in cheap-bound order with early termination: once the
            // (ascending) bound cannot beat the best, neither can anything
            // after it — the expensive refinement runs on a handful of
            // segments instead of the whole linework. Ties keep the lowest
            // segment index, exactly like the linear scan's first-wins.
            // Heapify + pop instead of a full sort: pruning usually stops
            // after a few lowest-bound candidates.
            #[derive(Clone, Copy)]
            struct BoundEntry {
                bound: f64,
                index: u32,
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
            // Max-heap inverted: smaller bound (then smaller index) pops first.
            impl Ord for BoundEntry {
                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                    other
                        .bound
                        .total_cmp(&self.bound)
                        .then_with(|| other.index.cmp(&self.index))
                }
            }
            let mut entries = Vec::new();
            crate::boundary::try_reserve_vec(&mut entries, self.segments.len())?;
            for (index, (segment, &offset)) in
                std::iter::zip(&self.segments, &self.prefix).enumerate()
            {
                let length = self.prefix[index + 1] - offset;
                let bound = metric.locate_bound(probe, segment.start, segment.end, length)?;
                crate::boundary::try_push_vec(&mut entries, BoundEntry {
                    bound,
                    index: index as u32,
                })?;
            }
            // O(n) heapify; pop ascending-bound until the ceiling is met.
            let mut order = std::collections::BinaryHeap::from(entries);
            let mut best_index = u32::MAX;
            while let Some(BoundEntry { bound, index }) = order.pop() {
                if bound > best_distance {
                    break;
                }
                // An equal-bound segment can still TIE exactly; first-wins
                // ordering only needs it refined when its index could win.
                #[expect(clippy::float_cmp, reason = "exact tie against the refined best")]
                let skippable_tie = bound == best_distance && index > best_index;
                if skippable_tie {
                    continue;
                }
                let segment = self.segments[index as usize];
                let offset = self.prefix[index as usize];
                let length = self.prefix[index as usize + 1] - offset;
                let Some((distance, along)) = metric.locate(
                    probe,
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
            for (index, (segment, &offset)) in
                std::iter::zip(&self.segments, &self.prefix).enumerate()
            {
                let length = self.prefix[index + 1] - offset;
                let Some((distance, along)) = metric.locate(
                    probe,
                    segment.start,
                    segment.end,
                    length,
                    best_distance,
                    &mut segment_scratch,
                )?
                else {
                    continue;
                };
                if distance < best_distance {
                    best_distance = distance;
                    best_offset = offset + along;
                }
            }
        }
        if normalized {
            Ok(best_offset / self.total)
        } else {
            Ok(best_offset)
        }
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
        self.segments.heap_bytes() + self.prefix.heap_bytes()
    }
}

fn try_boxed_slice<T>(mut values: Vec<T>) -> Result<Box<[T]>> {
    if values.try_shrink_to_fit().is_err() {
        drop(values);
        return Err(crate::error::ResourceError::Allocation.into());
    }
    if values.capacity() == values.len() {
        // With no spare capacity this conversion only transfers ownership and
        // cannot request another allocation.
        return Ok(values.into_boxed_slice());
    }
    // An allocator may retain padding after a successful shrink. Move into an
    // exact fallible box rather than letting `into_boxed_slice` retry that
    // shrink infallibly.
    let len = values.len();
    let Ok(mut boxed) = Box::<[T]>::try_new_uninit_slice(len) else {
        drop(values);
        return Err(crate::error::ResourceError::Allocation.into());
    };
    for (slot, value) in boxed.iter_mut().zip(values) {
        slot.write(value);
    }
    // SAFETY: the loop moved one initialized value into every slot.
    Ok(unsafe { boxed.assume_init() })
}

fn try_push_distinct_point(points: &mut Vec<Point>, point: Point) -> Result<()> {
    if points
        .last()
        .is_none_or(|last| !crate::geometry::same_point(*last, point))
    {
        crate::boundary::try_push_vec(points, point)?;
    }
    Ok(())
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
