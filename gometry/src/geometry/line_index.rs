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
    CoordSeq, Coordinates as _, GeodesicMetric, GeometryErrorKind, LineSeq, MeasureRange, Ordering,
    Point, Segment, Shape, lerp_point, linework_first_point, normalize_line_distance,
    point_distance, push_distinct_point, segment_projection,
};

/// Per-segment measurement for the linear-referencing engine.
pub trait SegmentMetric {
    /// Length of the segment `start`–`end` in this metric.
    fn length(&self, start: Point, end: Point) -> f64;
    /// The point at `fraction` (`[0, 1]`) along the segment.
    fn interpolate(&self, start: Point, end: Point, fraction: f64) -> Point;
    /// Whether locate sweeps should refine segments in cheap-lower-bound
    /// order with early termination (worth it only when refinement is
    /// expensive — the geodesic golden-section; planar projection is cheaper
    /// than the ordering would be).
    const ORDERED_LOCATE: bool = false;

    /// Cheap lower bound on this segment's locate distance (`0.0` when the
    /// metric has none). Sound: never exceeds the true distance.
    fn locate_bound(&self, _probe: Point, _start: Point, _end: Point, _length: f64) -> f64 {
        0.0
    }

    /// `(distance to probe, along-segment offset of the projection)`.
    /// `length` is the segment's cached metric length and `best` the running
    /// minimum distance: metrics with expensive refinement (geodesic
    /// golden-section) may return any distance `>= best` once a cheap bound
    /// proves the segment cannot improve on it.
    fn locate(&self, probe: Point, start: Point, end: Point, length: f64, best: f64) -> (f64, f64);
}

/// Planar (coordinate-unit) metric.
pub(crate) struct PlanarMetric;

impl SegmentMetric for PlanarMetric {
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
    ) -> (f64, f64) {
        // Projection classification, witness placement, and along-segment
        // reconstruction share the segment kernel's certified owner. The
        // exact fallback retains its ratio until `interpolate_xy` / `scale`,
        // so an interior parameter that rounds to an endpoint cannot erase
        // either the residual distance or the locate offset.
        let segment = Segment {
            start: start.into(),
            end: end.into(),
        };
        let projection = segment_projection(probe, segment);
        let foot = projection.interpolate_xy(segment);
        let ex = probe.x - foot.x;
        let ey = probe.y - foot.y;
        let squared = ex * ex + ey * ey;
        let distance =
            if crate::geometry::squared_norm_is_trustworthy(squared, ex == 0.0 && ey == 0.0) {
                squared.sqrt()
            } else {
                ex.hypot(ey)
            };
        // No outer finite-rescue on `distance`: it can only go non-finite
        // when a DELTA itself overflows (segment span or probe offset past
        // ~1.8e308) — and a segment that long has an infinite metric length,
        // which already poisons the prefix table (old path included), making
        // every LRS result on such inputs degenerate. The ~1e150-class
        // extremes the suite covers resolve correctly through the scaled
        // projection + `hypot` rescues above; guarding the last class was
        // measured at +14% on every real locate.
        (distance, projection.scale(length))
    }
}

/// Any [`GeodesicMetric`] is a [`SegmentMetric`] (lengths in meters).
impl<M: GeodesicMetric> SegmentMetric for M {
    const ORDERED_LOCATE: bool = true;

    fn locate_bound(&self, probe: Point, start: Point, end: Point, length: f64) -> f64 {
        // Triangle inequality off the auxiliary-sphere endpoint bounds: the
        // foot is at least the farther-bounded endpoint minus the segment.
        (self
            .point_distance_lower_bound(probe, start)
            .max(self.point_distance_lower_bound(probe, end))
            - length)
            .max(0.0)
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
    ) -> (f64, f64) {
        self.locate_on_segment(probe, self.make_segment(start, end), best)
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
    Invalid(Box<str>),
}

impl LineIndexSlot {
    pub(crate) fn build(shape: &Shape, metric: &impl SegmentMetric) -> Self {
        use crate::error::ErrorKind;
        match LineIndex::build(shape, metric) {
            Ok(index) => Self::Index(index),
            Err(error) => match error.kind() {
                ErrorKind::Geometry(GeometryErrorKind::EmptyLinework) => Self::Empty,
                ErrorKind::Geometry(GeometryErrorKind::LineStringRequired) => Self::NotLineal,
                other => Self::Invalid(format!("{other}").into()),
            },
        }
    }

    pub(crate) fn get(&self) -> Result<&LineIndex> {
        match self {
            Self::Index(index) => Ok(index),
            Self::NotLineal => Err(GeometryErrorKind::LineStringRequired.into()),
            Self::Empty => Err(GeometryErrorKind::EmptyLinework.into()),
            Self::Invalid(msg) => Err(GeometryErrorKind::message(msg.as_ref())),
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

pub struct LineIndex {
    /// Non-zero-length segments, in linework order.
    segments: Box<[OrdinateSegment]>,
    /// `prefix[i]` = cumulative length before segment `i`; the last entry is
    /// [`total`](Self::total). Lengths are cached here — locate's
    /// `mul_add(length, fraction, offset)` and every interpolation walk read
    /// them back instead of re-measuring.
    prefix: Box<[f64]>,
    total: f64,
    first: Point,
    last: Point,
}

impl LineIndex {
    /// Build over `shape`'s linework. `LineStringRequired` for non-lineal
    /// shapes, `EmptyLinework` when there is no first vertex (the zero-total
    /// degeneracies — all-zero-length linework — build fine and resolve to
    /// `first`, matching the walks).
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the metric is used only through its trait contract; a named generic adds no API signal"
    )]
    pub fn build(shape: &Shape, metric: &impl SegmentMetric) -> Result<Self> {
        let lines = shape.linework()?;
        let first = linework_first_point(&lines).ok_or(GeometryErrorKind::EmptyLinework)?;
        let upper = shape.segment_count();
        let mut segments = Vec::with_capacity(upper);
        let mut prefix = Vec::with_capacity(upper + 1);
        prefix.push(0.0);
        let mut total = 0.0;
        let mut last = first;
        for line in &lines {
            for [start, end] in line.segment_pairs() {
                last = end;
                let length = metric.length(start, end);
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
        Ok(Self {
            segments: segments.into_boxed_slice(),
            prefix: prefix.into_boxed_slice(),
            total,
            first,
            last,
        })
    }

    /// Build over one line's zero-copy [`CoordSeq`] window — the packed-`Lines`
    /// batch lane reads CSR rows through this with no per-row `Shape` / `Arc`
    /// traffic (the measured gap vs the `iter_rows` + `with_data` fallback).
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the metric is used only through its trait contract; a named generic adds no API signal"
    )]
    pub fn build_coordseq(coords: &CoordSeq, metric: &impl SegmentMetric) -> Result<Self> {
        let first = coords.first().ok_or(GeometryErrorKind::EmptyLinework)?;
        let upper = coords.len().saturating_sub(1);
        let mut segments = Vec::with_capacity(upper);
        let mut prefix = Vec::with_capacity(upper + 1);
        prefix.push(0.0);
        let mut total = 0.0;
        let mut last = first;
        for [start, end] in coords.segment_pairs() {
            last = end;
            let length = metric.length(start, end);
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
            segments.push(OrdinateSegment { start, end });
            prefix.push(total);
        }
        let last = coords.last().unwrap_or(last);
        Ok(Self {
            segments: segments.into_boxed_slice(),
            prefix: prefix.into_boxed_slice(),
            total,
            first,
            last,
        })
    }

    pub const fn total(&self) -> f64 {
        self.total
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
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the metric is used only through its trait contract; a named generic adds no API signal"
    )]
    pub fn interpolate(
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
            push_distinct_point(&mut result, at(from));
            push_distinct_point(&mut result, at(to));
        }

        Ok(match result.len() {
            0 => Shape::Point(self.interpolate(start, false, metric)),
            1 => Shape::Point(result[0]),
            _ => Shape::LineString(
                LineSeq::try_new(CoordSeq::from(result))
                    .expect("substring with two or more vertices forms a line"),
            ),
        })
    }

    /// The along-line offset of `probe`'s nearest projection (the engine
    /// behind `line_locate_point`). First-wins tie ordering, like the scan.
    pub fn locate_point<M: SegmentMetric>(
        &self,
        probe: Point,
        normalized: bool,
        metric: &M,
    ) -> f64 {
        if self.total == 0.0 {
            return 0.0;
        }
        let mut best_distance = f64::INFINITY;
        let mut best_offset = 0.0;
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
            let mut entries = Vec::with_capacity(self.segments.len());
            for (index, (segment, &offset)) in
                std::iter::zip(&self.segments, &self.prefix).enumerate()
            {
                let length = self.prefix[index + 1] - offset;
                let bound = metric.locate_bound(probe, segment.start, segment.end, length);
                entries.push(BoundEntry {
                    bound,
                    index: index as u32,
                });
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
                let (distance, along) =
                    metric.locate(probe, segment.start, segment.end, length, best_distance);
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
                let (distance, along) =
                    metric.locate(probe, segment.start, segment.end, length, best_distance);
                if distance < best_distance {
                    best_distance = distance;
                    best_offset = offset + along;
                }
            }
        }
        if normalized {
            best_offset / self.total
        } else {
            best_offset
        }
    }
}

impl HeapSize for LineIndexSlot {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Index(index) => index.heap_bytes(),
            Self::NotLineal | Self::Empty => 0,
            Self::Invalid(msg) => msg.len(),
        }
    }
}

impl HeapSize for LineIndex {
    fn heap_bytes(&self) -> usize {
        self.segments.heap_bytes() + self.prefix.heap_bytes()
    }
}
