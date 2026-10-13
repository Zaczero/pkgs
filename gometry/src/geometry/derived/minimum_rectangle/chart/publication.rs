//! Retained-chart boundary sampling and certification.

use super::super::corners::minimum_rectangle_not_representable;
use super::super::{
    ChartRectangleCorner, ComputedEdgeIntent, ComputedSampleOrdinates, ComputedVertexIntent,
    CoordSeq, ExactChartPoint, ExactRational, ExactSign, ExpansionBudget, GeographicFrame,
    GeometryErrorKind, MOrdinate, Point, Result, RetainedComputedSample, ZOrdinate,
    retained_site_longitude,
};
use super::corners::exact_chart_orientation;

pub(in crate::geometry::derived::minimum_rectangle) struct PublishedChartBoundary {
    corners: [Option<ExactChartPoint>; 4],
    last: Option<ExactChartPoint>,
    first_record: Option<Point>,
    first_direction: Option<(ExactRational, ExactRational)>,
    last_direction: Option<(ExactRational, ExactRational)>,
    last_sample: Option<(usize, f64)>,
    nonzero_edges: usize,
    direction_wraps: usize,
}

impl PublishedChartBoundary {
    pub(in crate::geometry::derived::minimum_rectangle) const fn new() -> Self {
        Self {
            corners: [None, None, None, None],
            last: None,
            first_record: None,
            first_direction: None,
            last_direction: None,
            last_sample: None,
            nonzero_edges: 0,
            direction_wraps: 0,
        }
    }

    fn counterexample(
        edge: usize,
        parameter: f64,
        point: Point,
        claim: &str,
    ) -> crate::error::Error {
        GeometryErrorKind::Invalid(
            format!(
                "published minimum_rotated_rectangle boundary {claim} at edge {edge}, t={parameter}: ({}, {})",
                point.x, point.y,
            )
            .into(),
        )
        .into()
    }

    #[expect(
        clippy::float_cmp,
        reason = "the publisher reports exact root parameters for corner endpoints and ring closure"
    )]
    fn record(
        &mut self,
        edge: usize,
        parameter: f64,
        sample: &RetainedComputedSample,
        point: Point,
    ) -> Result<()> {
        if self.last_sample == Some((3, 1.0))
            || edge >= 4
            || !(parameter.is_finite() && (0.0..=1.0).contains(&parameter))
        {
            return Err(Self::counterexample(
                edge,
                parameter,
                point,
                "has invalid root provenance",
            ));
        }
        if let Some((previous_edge, previous_parameter)) = self.last_sample {
            if edge < previous_edge
                || (edge == previous_edge && parameter < previous_parameter)
                || (edge > previous_edge
                    && (edge != previous_edge.wrapping_add(1) || previous_parameter != 1.0))
            {
                return Err(Self::counterexample(
                    edge,
                    parameter,
                    point,
                    "does not follow the directed root-edge order",
                ));
            }
        } else if edge != 0 || parameter != 0.0 {
            return Err(Self::counterexample(
                edge,
                parameter,
                point,
                "does not begin at the first root endpoint",
            ));
        }

        let exact = ExactChartPoint {
            x: sample.chart_longitude.clone(),
            y: ExactRational::from_f64(point.y),
        };
        let corner_index = if parameter == 0.0 {
            Some(edge)
        } else if parameter == 1.0 {
            Some(if edge == 3 { 0 } else { edge.wrapping_add(1) })
        } else {
            None
        };
        if let Some(index) = corner_index {
            let slot = self
                .corners
                .get_mut(index)
                .expect("rectangle root endpoint names one of its four corners");
            if slot.as_ref().is_some_and(|previous| previous != &exact) {
                return Err(Self::counterexample(
                    edge,
                    parameter,
                    point,
                    "does not preserve one exact corner across adjacent edges",
                ));
            }
            *slot = Some(exact.clone());
        }

        let is_closure = edge == 3 && parameter == 1.0;
        if self.last_sample.is_none() {
            self.first_record = Some(point);
            self.last = Some(exact);
        } else {
            let first = self
                .corners
                .first()
                .and_then(Option::as_ref)
                .expect("the first sample records rectangle corner zero");
            if exact == *first && !is_closure {
                return Err(Self::counterexample(
                    edge,
                    parameter,
                    point,
                    "returns to its start before ring closure",
                ));
            }
            if is_closure && (exact != *first || Some(point) != self.first_record) {
                return Err(Self::counterexample(
                    edge,
                    parameter,
                    point,
                    "does not close with the first published record",
                ));
            }

            let previous = self
                .last
                .as_ref()
                .expect("a recorded boundary has its previous point");
            let direction = (
                exact.x.try_subtract(&previous.x)?,
                exact.y.try_subtract(&previous.y)?,
            );
            if direction.0.sign() != ExactSign::Zero || direction.1.sign() != ExactSign::Zero {
                if let Some(previous_direction) = &self.last_direction {
                    let turn = vector_cross(previous_direction, &direction)?;
                    if turn == ExactSign::Negative {
                        return Err(Self::counterexample(
                            edge,
                            parameter,
                            point,
                            "is non-convex",
                        ));
                    }
                    let dot = previous_direction
                        .0
                        .try_product(&direction.0)?
                        .try_add(&previous_direction.1.try_product(&direction.1)?)?;
                    if turn == ExactSign::Zero && dot.sign() == ExactSign::Negative {
                        return Err(Self::counterexample(
                            edge,
                            parameter,
                            point,
                            "reverses along a collinear boundary",
                        ));
                    }
                    if vector_quadrant(previous_direction) > vector_quadrant(&direction) {
                        self.direction_wraps = self.direction_wraps.wrapping_add(1);
                    }
                } else {
                    self.first_direction = Some(direction.clone());
                }
                self.last_direction = Some(direction);
                self.nonzero_edges = self.nonzero_edges.wrapping_add(1);
            }
            self.last = Some(exact);
        }

        self.last_sample = Some((edge, parameter));
        Ok(())
    }

    fn finish(
        self,
        hull: &[crate::geographic::ChartPointSite],
        frame: GeographicFrame,
    ) -> Result<()> {
        if self.last_sample != Some((3, 1.0)) || self.nonzero_edges < 3 {
            return Err(GeometryErrorKind::Invalid(
                "published minimum_rotated_rectangle boundary is not a closed area".into(),
            )
            .into());
        }
        let first_direction = self
            .first_direction
            .as_ref()
            .expect("a boundary with three edges has a first direction");
        let last_direction = self
            .last_direction
            .as_ref()
            .expect("a boundary with three edges has a last direction");
        let closing_turn = vector_cross(last_direction, first_direction)?;
        if closing_turn == ExactSign::Negative {
            return Err(GeometryErrorKind::Invalid(
                "published minimum_rotated_rectangle boundary is non-convex at closure".into(),
            )
            .into());
        }
        if closing_turn == ExactSign::Zero {
            let closing_dot = last_direction
                .0
                .try_product(&first_direction.0)?
                .try_add(&last_direction.1.try_product(&first_direction.1)?)?;
            if closing_dot.sign() == ExactSign::Negative {
                return Err(GeometryErrorKind::Invalid(
                    "published minimum_rotated_rectangle boundary reverses at closure".into(),
                )
                .into());
            }
        }
        let direction_wraps = self.direction_wraps.wrapping_add(usize::from(
            vector_quadrant(last_direction) > vector_quadrant(first_direction),
        ));
        if direction_wraps != 1 {
            return Err(GeometryErrorKind::Invalid(
                "published minimum_rotated_rectangle boundary does not make one convex circuit"
                    .into(),
            )
            .into());
        }

        let [Some(first), Some(second), Some(third), Some(fourth)] = self.corners else {
            return Err(GeometryErrorKind::Invalid(
                "published minimum_rotated_rectangle boundary omitted a support corner".into(),
            )
            .into());
        };
        certify_chart_rectangle(hull, [&first, &second, &third, &fourth], frame)?;
        Ok(())
    }
}

pub(in crate::geometry::derived::minimum_rectangle) fn vector_cross(
    left: &(ExactRational, ExactRational),
    right: &(ExactRational, ExactRational),
) -> Result<ExactSign> {
    Ok(left
        .0
        .try_product(&right.1)?
        .try_subtract(&left.1.try_product(&right.0)?)?
        .sign())
}

pub(in crate::geometry::derived::minimum_rectangle) fn vector_quadrant(
    vector: &(ExactRational, ExactRational),
) -> u8 {
    use crate::geometry::numerics::product::ExactSign::Negative;

    let x = vector.0.sign();
    let y = vector.1.sign();
    if y == Negative {
        if x == Negative { 2 } else { 3 }
    } else {
        u8::from(x == Negative)
    }
}

pub(in crate::geometry::derived::minimum_rectangle) fn certify_chart_rectangle(
    hull: &[crate::geographic::ChartPointSite],
    corners: [&ExactChartPoint; 4],
    frame: GeographicFrame,
) -> Result<()> {
    let [first, second, third, fourth] = corners;
    let sides = [
        (first, second, third),
        (second, third, fourth),
        (third, fourth, first),
        (fourth, first, second),
    ];
    for (side, (start, next, opposite)) in sides.iter().copied().enumerate() {
        if exact_chart_orientation(start, next, opposite)? != ExactSign::Positive {
            return Err(GeometryErrorKind::Invalid(
                format!(
                    "published minimum_rotated_rectangle support side {side} is not a convex corner edge"
                )
                .into(),
            )
            .into());
        }
    }

    // One source-hull pass checks every actual published directed corner side.
    for site in hull {
        let longitude = retained_site_longitude(frame.longitude(), site.longitude())?;
        let source = ExactChartPoint {
            x: ExactRational::from_dyadic(longitude),
            y: ExactRational::from_f64(site.source_y()),
        };
        for (side, (start, next, _)) in sides.iter().copied().enumerate() {
            if exact_chart_orientation(start, next, &source)? == ExactSign::Negative {
                return Err(GeometryErrorKind::Invalid(
                    format!(
                        "published minimum_rotated_rectangle excludes source hull ordinal {} at side {side}: ({}, {})",
                        site.source_ordinal(),
                        site.longitude().native().get(),
                        site.source_y(),
                    )
                    .into(),
                )
                .into());
            }
        }
    }
    Ok(())
}

pub(in crate::geometry::derived::minimum_rectangle) fn publish_and_certify_chart_rectangle(
    hull: &[crate::geographic::ChartPointSite],
    corners: &[ChartRectangleCorner; 4],
    frame: GeographicFrame,
) -> Result<CoordSeq> {
    let first = corners
        .first()
        .expect("a chart rectangle has four constructed corners")
        .point;
    let mut points = corners
        .iter()
        .map(|corner| corner.point)
        .collect::<Vec<_>>();
    points.push(first);
    let mut budget = ExpansionBudget::new("minimum_rotated_rectangle", "computed edges");
    let mut boundary = PublishedChartBoundary::new();
    let published = crate::geographic::publish_computed_chain_with_vertex_intents_and_samples(
        frame,
        points.as_slice(),
        true,
        &mut budget,
        |edge, _, _| {
            let next = if edge == 3 { 0 } else { edge.wrapping_add(1) };
            let start = corners
                .get(edge)
                .ok_or_else(|| GeometryErrorKind::Invalid("unknown rectangle edge".into()))?;
            let end = corners
                .get(next)
                .ok_or_else(|| GeometryErrorKind::Invalid("unknown rectangle edge end".into()))?;
            Ok(ComputedEdgeIntent::exact_lifted([
                &start.exact.x,
                &end.exact.x,
            ]))
        },
        |_, _| ComputedVertexIntent::Sampled,
        |edge, _, _, parameter, _| {
            let next = if edge == 3 { 0 } else { edge.wrapping_add(1) };
            let start = corners.get(edge).ok_or_else(|| {
                GeometryErrorKind::Invalid("unknown rectangle sample edge".into())
            })?;
            let end = corners
                .get(next)
                .ok_or_else(|| GeometryErrorKind::Invalid("unknown rectangle sample end".into()))?;
            let latitude_delta = end.exact.y.try_subtract(&start.exact.y)?;
            let exact_parameter = ExactRational::from_f64(parameter);
            let latitude = start
                .exact
                .y
                .try_add(&latitude_delta.try_product(&exact_parameter)?)?
                .round()?;
            if !latitude.is_finite() {
                return Err(minimum_rectangle_not_representable());
            }
            Ok(ComputedSampleOrdinates {
                y: latitude,
                z: ZOrdinate(None),
                m: MOrdinate(None),
            })
        },
        |edge, _, _, parameter, retained, point| boundary.record(edge, parameter, retained, point),
    )?
    .ok_or_else(|| {
        GeometryErrorKind::Invalid(
            "sampled chart rectangle unexpectedly required no publication".into(),
        )
    })?;
    boundary.finish(hull, frame)?;
    CoordSeq::try_from_points(&published)
}
