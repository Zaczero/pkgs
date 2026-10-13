use super::{
    ChartLineIndex, ChartMetric, ChartParts, CoordSeq, GeodesicMetric, GeometryErrorKind,
    InterpolationProvenance, LineIndex, LineSegments, LineSeq, MeasureRange, Ordering, Point,
    PointEdgeScratch, Result, SegmentMetric, Shape, normalize_line_distance, point_chart_locate,
    push_distinct_active_position,
};

impl ChartLineIndex {
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
            self.segments.append_embedded_interval(
                index,
                metric,
                length,
                (from - segment_start) / length,
                (to - segment_start) / length,
                &mut result,
            )?;
        }

        Ok(match result.as_slice() {
            [] => Shape::Point(self.interpolate_route_position(start, false, metric)?),
            [point] => Shape::Point(*point),
            _ => Shape::LineString(LineSeq::from_proven(CoordSeq::try_from_points(&result)?)),
        })
    }
}
