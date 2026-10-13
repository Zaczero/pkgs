use super::*;

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

    #[expect(
        clippy::float_cmp,
        reason = "substring boundaries at exact prefix endpoints preserve authentic source points"
    )]
    fn append_embedded_interval(
        &self,
        start: Point,
        end: Point,
        length: f64,
        from_fraction: f64,
        to_fraction: f64,
        output: &mut Vec<Point>,
    ) -> Result<()> {
        let from = if from_fraction == 0.0 {
            start
        } else if from_fraction == 1.0 {
            end
        } else {
            self.interpolate(
                start,
                end,
                length,
                from_fraction,
                InterpolationProvenance::Embedded,
            )?
        };
        let to = if to_fraction == 0.0 {
            start
        } else if to_fraction == 1.0 {
            end
        } else {
            self.interpolate(
                start,
                end,
                length,
                to_fraction,
                InterpolationProvenance::Embedded,
            )?
        };
        push_distinct_active_position(output, from);
        push_distinct_active_position(output, to);
        Ok(())
    }

    fn append_embedded_prepared_interval(
        &self,
        part: &ChartEdgePart,
        length: f64,
        from_fraction: f64,
        to_fraction: f64,
        output: &mut Vec<Point>,
    ) -> Result<()> {
        self.append_embedded_interval(
            part.start,
            part.end,
            length,
            from_fraction,
            to_fraction,
            output,
        )
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

    fn append_embedded_interval(
        &self,
        start: Point,
        end: Point,
        length: f64,
        from_fraction: f64,
        to_fraction: f64,
        output: &mut Vec<Point>,
    ) -> Result<()> {
        let edge = self.chart_edge(start, end)?;
        append_chart_embedded_interval(
            self,
            start,
            end,
            &edge,
            length,
            from_fraction,
            to_fraction,
            output,
        )
    }

    fn append_embedded_prepared_interval(
        &self,
        part: &ChartEdgePart,
        length: f64,
        from_fraction: f64,
        to_fraction: f64,
        output: &mut Vec<Point>,
    ) -> Result<()> {
        append_chart_embedded_interval(
            self,
            part.start,
            part.end,
            &part.edge,
            length,
            from_fraction,
            to_fraction,
            output,
        )
    }
}

#[expect(
    clippy::float_cmp,
    reason = "substring endpoints at exact source parameters preserve authentic vertices"
)]
fn append_chart_embedded_interval<M: ChartMetric>(
    metric: &M,
    start: Point,
    end: Point,
    edge: &ChartEdge,
    length: f64,
    from_fraction: f64,
    to_fraction: f64,
    output: &mut Vec<Point>,
) -> Result<()> {
    let parameter_at = |fraction: f64| {
        if fraction == 0.0 {
            Ok(0.0)
        } else if fraction == 1.0 {
            Ok(1.0)
        } else {
            metric.chart_edge_locate_t(edge, length * fraction, length)
        }
    };

    let from = parameter_at(from_fraction)?;
    let to = parameter_at(to_fraction)?;
    edge.publish_source_interval(
        start,
        end,
        from,
        to,
        |parameter| metric.chart_edge_witness(start, end, edge, parameter, Some(length)),
        |_, point, _| {
            push_distinct_active_position(output, point);
            Ok(())
        },
        "line_substring",
    )?;
    Ok(())
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
    edge.source_sheet_point(start, end, parameter, point)
}
