use super::output::OutputColumns;
use super::plan::Route;
use super::routes::point_from_xy;
use super::*;
use crate::geometry::ordinates::{RootMetricState, resolve_source_root};
use crate::geometry::tessellation::exact::ExactRational;
use crate::work::Unmetered;
#[derive(Default)]
pub(crate) struct ChartSourceParameters {
    /// Cumulative parameter-vector ends for each original chart edge.
    pub(crate) edge_ends: Vec<usize>,
    /// The emitted binary64 source parameters, grouped per original edge.
    /// Each edge's sequence starts at `0.0` and ends at `1.0`.
    pub(crate) parameters: Vec<f64>,
}

pub(super) struct Chart<'a> {
    pub(super) geodesic: &'a geographiclib_rs::Geodesic,
    pub(super) frame: GeographicFrame,
}

/// A chart route used only by clearance support, where emitted root parameters
/// are needed alongside the prepared coordinates.
pub(super) struct ChartWithSourceParameters<'a> {
    chart: Chart<'a>,
    parameters: Vec<f64>,
    edge_ends: Vec<usize>,
    next_edge: usize,
    edge_count: usize,
}

impl<'a> ChartWithSourceParameters<'a> {
    pub(super) fn new(
        geodesic: &'a geographiclib_rs::Geodesic,
        frame: GeographicFrame,
        edge_count: usize,
    ) -> Self {
        Self {
            chart: Chart { geodesic, frame },
            parameters: Vec::new(),
            edge_ends: Vec::with_capacity(edge_count),
            next_edge: 0,
            edge_count,
        }
    }
}

impl<'metric> Chart<'metric> {
    fn emission<'source>(
        &self,
        segment: Segment,
        length: f64,
        pieces: usize,
        source: &'source CoordSeq,
        source_segment: usize,
    ) -> Result<ChartEmission<'source, 'metric>> {
        let edge = crate::crs::ChartEdge::new(
            point_from_xy(segment.start),
            point_from_xy(segment.end),
            self.frame,
        )?;
        Ok(ChartEmission {
            source,
            source_segment,
            edge,
            geodesic: self.geodesic,
            length,
            pieces,
        })
    }
}

struct ChartEmission<'source, 'metric> {
    source: &'source CoordSeq,
    source_segment: usize,
    edge: crate::crs::ChartEdge,
    geodesic: &'metric geographiclib_rs::Geodesic,
    length: f64,
    pieces: usize,
}

impl ChartEmission<'_, '_> {
    #[expect(
        clippy::float_cmp,
        reason = "source parameters are exact binary64 vertices of the emitted chain"
    )]
    fn emit(
        &self,
        output: &mut OutputColumns,
        mut record_parameter: impl FnMut(f64),
        budget: &ExpansionBudget,
        planned_generated: usize,
        refined_generated: &mut usize,
    ) -> Result<()> {
        let source_start = self.source.nth_coord(self.source_segment);
        let source_end = self.source.nth_coord(self.source_segment.wrapping_add(1));
        let source = [source_start, source_end];
        let axes = self.source.axes();
        let metric =
            crate::crs::EllipsoidMetric::for_geodesic_at_frame(self.geodesic, 0, self.edge.frame());
        let mut metric_state = RootMetricState::from_admitted_edge(self.edge, Some(self.length));
        let mut work = Unmetered;
        let mut previous_parameter = 0.0;
        let mut last_emitted_parameter = None;
        for step in 1..=self.pieces {
            let fraction = step as f64 / self.pieces as f64;
            let parameter = if step == self.pieces {
                1.0
            } else if self.edge.is_parallel() {
                fraction
            } else {
                self.edge
                    .locate_t(self.length * fraction, self.length, self.geodesic)?
            };
            if parameter <= previous_parameter || (step < self.pieces && parameter >= 1.0) {
                return Err(GeometryErrorKind::unrepresentable_output("segmentize"));
            }
            self.edge.publish_source_interval(
                source_start,
                source_end,
                previous_parameter,
                parameter,
                |parameter| self.edge.chart_position(parameter),
                |parameter, point, refined| {
                    if parameter == 0.0
                        || parameter == 1.0
                        || last_emitted_parameter == Some(parameter)
                    {
                        return Ok(());
                    }
                    if !(point.x.is_finite() && point.y.is_finite()) {
                        return Err(GeometryErrorKind::message(
                            "chart-edge subdivision calculation produced a non-finite coordinate",
                        ));
                    }
                    let chart_parameter = ExactRational::from_f64(parameter);
                    let mut fraction_slot = None;
                    let ordinates = resolve_source_root(
                        self.edge.frame(),
                        &source,
                        self.edge.retained_displacement(),
                        &chart_parameter,
                        axes,
                        || Ok(&metric),
                        &mut metric_state,
                        &mut fraction_slot,
                        &mut work,
                    )?;
                    if refined {
                        let additional = refined_generated
                            .checked_add(1)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                        let requested = planned_generated
                            .checked_add(additional)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                        budget.check_additional(requested)?;
                        *refined_generated = additional;
                    }
                    output.push_point(Point::new_axes(
                        point.x,
                        point.y,
                        crate::geometry::ZOrdinate(ordinates.z.value()),
                        crate::geometry::MOrdinate(ordinates.m.value()),
                    )?)?;
                    record_parameter(parameter);
                    last_emitted_parameter = Some(parameter);
                    Ok(())
                },
                "segmentize",
            )
            .map_err(|error| {
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        GeometryErrorKind::UnrepresentableOutput {
                            operation: "computed edge",
                            ..
                        }
                    )
                ) {
                    GeometryErrorKind::unrepresentable_output("segmentize")
                } else {
                    error
                }
            })?;
            previous_parameter = parameter;
        }
        Ok(())
    }
}

impl Route for Chart<'_> {
    type Solution = f64;

    fn solve(&self, segment: Segment, _target: Target<'_>) -> Result<(f64, Self::Solution)> {
        let edge = crate::crs::ChartEdge::new(
            point_from_xy(segment.start),
            point_from_xy(segment.end),
            self.frame,
        )?;
        let length = edge.length(self.geodesic)?;
        Ok((length, length))
    }

    fn interior(
        &mut self,
        segment: Segment,
        length: Self::Solution,
        pieces: usize,
        source: &CoordSeq,
        source_segment: usize,
        output: &mut OutputColumns,
        budget: &ExpansionBudget,
        planned_generated: usize,
        refined_generated: &mut usize,
    ) -> Result<()> {
        self.emission(segment, length, pieces, source, source_segment)?
            .emit(output, |_| {}, budget, planned_generated, refined_generated)
    }

    fn measure(&self, segment: Segment) -> Result<f64> {
        crate::crs::ChartEdge::new(
            point_from_xy(segment.start),
            point_from_xy(segment.end),
            self.frame,
        )?
        .length(self.geodesic)
    }
}

impl ChartWithSourceParameters<'_> {
    fn append_whole_edge(&mut self) -> Result<()> {
        if self.next_edge >= self.edge_count {
            return Err(GeometryErrorKind::Invalid(
                "chart source parameter edge exceeds the input chain".into(),
            )
            .into());
        }
        self.parameters.extend([0.0, 1.0]);
        self.edge_ends.push(self.parameters.len());
        self.next_edge = self
            .next_edge
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<ChartSourceParameters> {
        while self.next_edge < self.edge_count {
            self.append_whole_edge()?;
        }
        if self.next_edge != self.edge_count || self.edge_ends.len() != self.edge_count {
            return Err(GeometryErrorKind::message(
                "chart source parameter edges do not match the input chain",
            ));
        }
        Ok(ChartSourceParameters {
            edge_ends: self.edge_ends,
            parameters: self.parameters,
        })
    }
}

impl Route for ChartWithSourceParameters<'_> {
    type Solution = f64;

    fn solve(&self, segment: Segment, target: Target<'_>) -> Result<(f64, Self::Solution)> {
        self.chart.solve(segment, target)
    }

    fn interior(
        &mut self,
        segment: Segment,
        length: Self::Solution,
        pieces: usize,
        source: &CoordSeq,
        source_segment: usize,
        output: &mut OutputColumns,
        budget: &ExpansionBudget,
        planned_generated: usize,
        refined_generated: &mut usize,
    ) -> Result<()> {
        if source_segment >= self.edge_count {
            return Err(GeometryErrorKind::Invalid(
                "chart source parameter references an unknown input edge".into(),
            )
            .into());
        }
        while self.next_edge < source_segment {
            self.append_whole_edge()?;
        }
        if self.next_edge != source_segment {
            return Err(GeometryErrorKind::Invalid(
                "chart source parameter edge order differs from segmentize".into(),
            )
            .into());
        }
        let Self {
            chart,
            parameters,
            edge_ends,
            next_edge,
            ..
        } = self;
        parameters.push(0.0);
        chart
            .emission(segment, length, pieces, source, source_segment)?
            .emit(
                output,
                |parameter| parameters.push(parameter),
                budget,
                planned_generated,
                refined_generated,
            )?;
        parameters.push(1.0);
        edge_ends.push(parameters.len());
        *next_edge = next_edge
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(())
    }

    fn measure(&self, segment: Segment) -> Result<f64> {
        self.chart.measure(segment)
    }
}
