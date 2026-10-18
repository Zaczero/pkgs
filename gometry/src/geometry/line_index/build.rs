use super::*;

/// The prefix-length index over one shape's linework, in one metric.
/// Each stored segment has FULL ordinate carry — the planar [`Segment`] is
/// XY-only by design, and LRS interpolates Z/M.
#[derive(Clone, Copy)]
pub(crate) struct OrdinateSegment {
    pub(super) start: Point,
    pub(super) end: Point,
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
    fn append_embedded_interval<M: SegmentMetric>(
        &self,
        index: usize,
        metric: &M,
        length: f64,
        from_fraction: f64,
        to_fraction: f64,
        output: &mut Vec<Point>,
    ) -> Result<()>;
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

    fn append_embedded_interval<M: SegmentMetric>(
        &self,
        index: usize,
        metric: &M,
        length: f64,
        from_fraction: f64,
        to_fraction: f64,
        output: &mut Vec<Point>,
    ) -> Result<()> {
        let segment = self.segment(index);
        metric.append_embedded_interval(
            segment.start,
            segment.end,
            length,
            from_fraction,
            to_fraction,
            output,
        )
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

    fn append_embedded_interval<M: SegmentMetric>(
        &self,
        index: usize,
        metric: &M,
        length: f64,
        from_fraction: f64,
        to_fraction: f64,
        output: &mut Vec<Point>,
    ) -> Result<()> {
        let part = self
            .edges
            .get(index)
            .expect("chart-edge ordinal belongs to the route");
        metric.append_embedded_prepared_interval(part, length, from_fraction, to_fraction, output)
    }
}

impl LineIndex {
    fn storage(upper: Option<usize>) -> (Vec<OrdinateSegment>, Vec<f64>) {
        let mut segments = Vec::new();
        let mut prefix = Vec::new();
        if let Some(upper) = upper {
            segments.reserve(upper);
            if let Some(prefix_capacity) = upper.checked_add(1) {
                prefix.reserve(prefix_capacity);
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
}
