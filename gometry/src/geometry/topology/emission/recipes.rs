use super::{
    ChartSpan, CompletedNativeDrawing, DirectedSpan, ExactChartPoint, ExactGeographicTopology,
    ExactQuotientKey, ExactRational, ExactSourceRange, FeatureFamily, FinishedGeometry,
    GeometryErrorKind, Latitude, NodeOrdinates, Point, QPortId, Range, RequestRowId, Result,
    SourceEdgeId, SourceTrace, TraceRequestLocation, compare_quotient_keys_with_work,
    quotient_key_for_chart_position_with_work, split_rational_native_site_with_work,
};
use crate::work::Work;

pub(in crate::geometry::topology) struct SealedSourceDrawing {
    pub(in crate::geometry::topology) sealed: FinishedGeometry,
}

pub(in crate::geometry::topology) enum SealedQDrawing {
    Source(SealedSourceDrawing),
    Native(CompletedNativeDrawing),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct FragmentRecipeId(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology) enum SelectedUse {
    AreaBoundary,
    ArealBoundaryLine,
    Curve,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct DirectedSourceRange {
    pub(in crate::geometry::topology) source_edge: SourceEdgeId,
    pub(in crate::geometry::topology) range: ExactSourceRange,
    pub(in crate::geometry::topology) emitted: bool,
    pub(in crate::geometry::topology) producer_start: ExactRational,
    pub(in crate::geometry::topology) producer_end: ExactRational,
    pub(in crate::geometry::topology) endpoint_only: bool,
}

pub(super) struct PendingSourceRange {
    pub(super) source_edge: SourceEdgeId,
    pub(super) range: ExactSourceRange,
    pub(super) emitted: bool,
    pub(super) longitude_start: ExactRational,
    pub(super) longitude_end: ExactRational,
    pub(super) latitude_start: ExactRational,
    pub(super) latitude_end: ExactRational,
}

pub(super) fn pending_source_ranges(
    span: &ChartSpan,
    directed: DirectedSpan,
    longitude_start: &ExactRational,
    longitude_end: &ExactRational,
    latitude_start: &ExactRational,
    latitude_end: &ExactRational,
    work: &mut impl Work,
) -> Result<Vec<PendingSourceRange>> {
    let mut output = Vec::with_capacity(span.owners.len());
    for (owner_index, owner) in span.owners.iter().enumerate() {
        work.charge_into_error(1)?;
        let forward = (directed.from == span.ports[0]) != owner.reversed;
        let range = if forward {
            ExactSourceRange {
                start: owner.source_range.start.try_clone_with_work(work)?,
                end: owner.source_range.end.try_clone_with_work(work)?,
            }
        } else {
            ExactSourceRange {
                start: owner.source_range.end.try_clone_with_work(work)?,
                end: owner.source_range.start.try_clone_with_work(work)?,
            }
        };
        output.push(PendingSourceRange {
            source_edge: owner.source_edge,
            range,
            emitted: owner_index == directed.owner_index,
            longitude_start: longitude_start.try_clone_with_work(work)?,
            longitude_end: longitude_end.try_clone_with_work(work)?,
            latitude_start: latitude_start.try_clone_with_work(work)?,
            latitude_end: latitude_end.try_clone_with_work(work)?,
        });
    }
    Ok(output)
}

pub(super) fn producer_mapping_is_affine(
    ranges: &[PendingSourceRange],
    longitude_displacement: &ExactRational,
    latitude_displacement: &ExactRational,
    support_allowed: bool,
    work: &mut impl Work,
) -> Result<bool> {
    let axis = if !longitude_displacement.is_zero() {
        Some((longitude_displacement, true))
    } else if !latitude_displacement.is_zero() {
        Some((latitude_displacement, false))
    } else {
        None
    };
    let Some((displacement, longitude)) = axis.filter(|_| support_allowed) else {
        return Ok(false);
    };
    {
        let zero = ExactRational::zero();
        let displacement_order = displacement.try_cmp_with_work(&zero, work)?;
        let mut supported = true;
        for range in ranges {
            work.charge_into_error(1)?;
            let longitude_step = range
                .longitude_end
                .try_subtract_with_work(&range.longitude_start, work)?;
            let latitude_step = range
                .latitude_end
                .try_subtract_with_work(&range.latitude_start, work)?;
            if !longitude_step
                .try_product_with_work(latitude_displacement, work)?
                .try_eq_with_work(
                    &latitude_step.try_product_with_work(longitude_displacement, work)?,
                    work,
                )?
            {
                supported = false;
                break;
            }
            let step = if longitude {
                &longitude_step
            } else {
                &latitude_step
            };
            let step_order = step.try_cmp_with_work(&zero, work)?;
            if (displacement_order.is_gt() && step_order.is_lt())
                || (displacement_order.is_lt() && step_order.is_gt())
            {
                supported = false;
                break;
            }
        }
        Ok(supported)
    }
}

pub(super) fn producer_step_extends_run(
    longitude_displacement: &ExactRational,
    latitude_displacement: &ExactRational,
    longitude_step: &ExactRational,
    latitude_step: &ExactRational,
    support_allowed: bool,
    point_support: bool,
    work: &mut impl Work,
) -> Result<bool> {
    work.charge_into_error(1)?;
    if !support_allowed
        || point_support
        || (longitude_displacement.is_zero() && latitude_displacement.is_zero())
    {
        return Ok(false);
    }
    // The admitted prefix is already affine to its cumulative displacement;
    // collinearity and a matching primary-axis sign certify only this step.
    let cross = longitude_displacement
        .try_product_with_work(latitude_step, work)?
        .try_subtract_with_work(
            &latitude_displacement.try_product_with_work(longitude_step, work)?,
            work,
        )?;
    if !cross.is_zero() {
        return Ok(false);
    }
    let zero = ExactRational::zero();
    let (displacement_primary, step_primary) = if longitude_displacement.is_zero() {
        (latitude_displacement, latitude_step)
    } else {
        (longitude_displacement, longitude_step)
    };
    let displacement_order = displacement_primary.try_cmp_with_work(&zero, work)?;
    let step_order = step_primary.try_cmp_with_work(&zero, work)?;
    Ok(!displacement_order.is_eq() && displacement_order == step_order)
}

pub(super) fn producer_ranges(
    ranges: Vec<PendingSourceRange>,
    longitude_displacement: &ExactRational,
    latitude_displacement: &ExactRational,
    support_allowed: bool,
    work: &mut impl Work,
) -> Result<Vec<DirectedSourceRange>> {
    let has_displacement = !longitude_displacement.is_zero() || !latitude_displacement.is_zero();
    let endpoint_only = !support_allowed || !has_displacement;
    let affine = producer_mapping_is_affine(
        &ranges,
        longitude_displacement,
        latitude_displacement,
        support_allowed,
        work,
    )?;
    if !endpoint_only && !affine {
        return Err(GeometryErrorKind::Invalid(
            "producer fragment changes direction before a written Q port".into(),
        )
        .into());
    }
    let mut output = Vec::with_capacity(ranges.len());
    for range in ranges {
        work.charge_into_error(1)?;
        let (producer_start, producer_end) = if endpoint_only {
            (ExactRational::zero(), ExactRational::one())
        } else if !longitude_displacement.is_zero() {
            let (start, end) = (&range.longitude_start, &range.longitude_end);
            (
                start.try_quotient_with_work(longitude_displacement, work)?,
                end.try_quotient_with_work(longitude_displacement, work)?,
            )
        } else if !latitude_displacement.is_zero() {
            let (start, end) = (&range.latitude_start, &range.latitude_end);
            (
                start.try_quotient_with_work(latitude_displacement, work)?,
                end.try_quotient_with_work(latitude_displacement, work)?,
            )
        } else {
            (ExactRational::zero(), ExactRational::one())
        };
        output.push(DirectedSourceRange {
            source_edge: range.source_edge,
            range: range.range,
            emitted: range.emitted,
            producer_start,
            producer_end,
            endpoint_only,
        });
    }
    Ok(output)
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct FragmentRecipe {
    pub(in crate::geometry::topology) id: FragmentRecipeId,
    pub(in crate::geometry::topology) endpoints: [QPortId; 2],
    pub(in crate::geometry::topology) ranges: Vec<DirectedSourceRange>,
    pub(in crate::geometry::topology) exact_displacement: ExactRational,
    pub(in crate::geometry::topology) use_kind: SelectedUse,
}

#[derive(Clone, Copy)]
pub(super) enum EndpointSide {
    Start,
    End,
}

impl EndpointSide {
    pub(super) const fn index(self) -> usize {
        match self {
            Self::Start => 0,
            Self::End => 1,
        }
    }

    pub(super) const fn parameter(self) -> ExactRational {
        match self {
            Self::Start => ExactRational::zero(),
            Self::End => ExactRational::one(),
        }
    }
}

pub(super) fn interpolate_exact_reference_coordinate(
    start: &ExactRational,
    end: &ExactRational,
    parameter: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactRational> {
    let displacement = end.try_subtract_with_work(start, work)?;
    start.try_add_with_work(&displacement.try_product_with_work(parameter, work)?, work)
}

pub(super) fn interpolate_exact_reference_point(
    endpoints: &[ExactChartPoint; 2],
    parameter: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactChartPoint> {
    Ok(ExactChartPoint::from_rationals(
        interpolate_exact_reference_coordinate(&endpoints[0].x, &endpoints[1].x, parameter, work)?,
        interpolate_exact_reference_coordinate(&endpoints[0].y, &endpoints[1].y, parameter, work)?,
    ))
}

pub(in crate::geometry::topology) struct PublishedSample {
    pub(in crate::geometry::topology) request_row: RequestRowId,
    pub(in crate::geometry::topology) family: FeatureFamily,
    pub(in crate::geometry::topology) parameter: ExactRational,
    pub(in crate::geometry::topology) written_vertex: usize,
    pub(in crate::geometry::topology) point: Point,
    pub(in crate::geometry::topology) exact_point: ExactChartPoint,
    pub(in crate::geometry::topology) published_chart_point: ExactChartPoint,
    pub(in crate::geometry::topology) source_trace: Option<TraceRequestLocation>,
    pub(in crate::geometry::topology) endpoint_only: bool,
}

pub(in crate::geometry::topology) struct PublishedFragment {
    pub(in crate::geometry::topology) recipe: FragmentRecipeId,
    pub(in crate::geometry::topology) points: Vec<Point>,
    pub(in crate::geometry::topology) trace: SourceTrace,
    pub(in crate::geometry::topology) sample_range: Range<usize>,
    pub(in crate::geometry::topology) source_records: [Option<Point>; 2],
    pub(in crate::geometry::topology) exact_endpoints: [ExactChartPoint; 2],
}

pub(in crate::geometry::topology) struct PublicationInventory {
    pub(in crate::geometry::topology) fragments: Vec<PublishedFragment>,
    pub(in crate::geometry::topology) samples: Vec<PublishedSample>,
    pub(in crate::geometry::topology) counts: Vec<usize>,
    pub(in crate::geometry::topology) ordinates: Vec<NodeOrdinates>,
}

impl FragmentRecipe {
    pub(in crate::geometry::topology) fn reference_endpoints(
        &self,
        topology: &ExactGeographicTopology,
        work: &mut impl Work,
    ) -> Result<[ExactChartPoint; 2]> {
        let start_port = topology
            .q
            .ports
            .get(self.endpoints[0].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("publication reference start port is unknown".into())
            })?;
        let end_port = topology
            .q
            .ports
            .get(self.endpoints[1].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("publication reference end port is unknown".into())
            })?;
        let start = start_port.point.try_clone_with_work(work)?;
        let end = ExactChartPoint::from_rationals(
            start.x.try_add_with_work(&self.exact_displacement, work)?,
            end_port.point.y.try_clone_with_work(work)?,
        );
        let period = ExactRational::from_f64(topology.sources.frame.longitude().period());
        let end_offset = end
            .x
            .try_subtract_with_work(&end_port.point.x, work)?
            .try_quotient_with_work(&period, work)?;
        if !end_offset.is_integer() {
            return Err(GeometryErrorKind::Invalid(
                "publication reference end is not an integral-period Q-port alias".into(),
            )
            .into());
        }
        if !quotient_key_for_chart_position_with_work(topology.sources.frame, &end, work)
            .and_then(|end_key| {
                let port_key = quotient_key_for_chart_position_with_work(
                    topology.sources.frame,
                    &end_port.point,
                    work,
                )?;
                compare_quotient_keys_with_work(&end_key, &port_key, work)
            })?
            .is_eq()
        {
            return Err(GeometryErrorKind::Invalid(
                "publication reference end disagrees with its Q-port identity".into(),
            )
            .into());
        }
        Ok([start, end])
    }

    pub(super) fn source_endpoint_record(
        &self,
        topology: &ExactGeographicTopology,
        side: EndpointSide,
        work: &mut impl Work,
    ) -> Result<Option<Point>> {
        let local_parameter = side.parameter();
        for range in &self.ranges {
            if !range.emitted {
                continue;
            }
            if range.endpoint_only
                && !local_parameter
                    .try_cmp_with_work(&range.producer_start, work)?
                    .is_eq()
                && !local_parameter
                    .try_cmp_with_work(&range.producer_end, work)?
                    .is_eq()
            {
                continue;
            }
            let producer_order = range
                .producer_start
                .try_cmp_with_work(&range.producer_end, work)?;
            let lower = if producer_order.is_le() {
                &range.producer_start
            } else {
                &range.producer_end
            };
            let upper = if producer_order.is_le() {
                &range.producer_end
            } else {
                &range.producer_start
            };
            if local_parameter.try_cmp_with_work(lower, work)?.is_lt()
                || local_parameter.try_cmp_with_work(upper, work)?.is_gt()
            {
                continue;
            }
            let width = range
                .producer_end
                .try_subtract_with_work(&range.producer_start, work)?;
            if width.is_zero() {
                continue;
            }
            let within = local_parameter
                .try_subtract_with_work(&range.producer_start, work)?
                .try_quotient_with_work(&width, work)?;
            if within
                .try_cmp_with_work(&ExactRational::zero(), work)?
                .is_lt()
                || within
                    .try_cmp_with_work(&ExactRational::one(), work)?
                    .is_gt()
            {
                continue;
            }
            let parameter = range.range.compose_with_work(&within, &within, work)?.start;
            let root_side = if parameter.is_zero() {
                Some(EndpointSide::Start)
            } else if parameter.is_one() {
                Some(EndpointSide::End)
            } else {
                None
            };
            let Some(root_side) = root_side else {
                continue;
            };
            let edge = topology
                .sources
                .edges
                .get(range.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("publication endpoint source edge is unknown".into())
                })?;
            let Some(record) = edge.source.get(root_side.index()).copied() else {
                continue;
            };
            let port_id = *self.endpoints.get(side.index()).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication endpoint port is unknown".into())
            })?;
            let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("publication endpoint Q port is unknown".into())
            })?;
            let admitted_y = Latitude::from_admitted(topology.sources.frame, record.y).get();
            let record_point = ExactChartPoint::from_rationals(
                ExactRational::from_f64(record.x),
                ExactRational::from_f64(admitted_y),
            );
            let record_key = quotient_key_for_chart_position_with_work(
                topology.sources.frame,
                &record_point,
                work,
            )?;
            let port_key = quotient_key_for_chart_position_with_work(
                topology.sources.frame,
                &port.point,
                work,
            )?;
            if !compare_quotient_keys_with_work(&record_key, &port_key, work)?.is_eq() {
                continue;
            }
            if matches!(
                &record_key,
                ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole
            ) {
                let longitude = topology.sources.frame.longitude();
                let (record_longitude, _) = split_rational_native_site_with_work(
                    &record_point.x,
                    longitude.period(),
                    longitude.half_period(),
                    work,
                )?;
                let (port_longitude, _) = split_rational_native_site_with_work(
                    &port.point.x,
                    longitude.period(),
                    longitude.half_period(),
                    work,
                )?;
                if !record_longitude
                    .as_ref()
                    .try_cmp_with_work(port_longitude.as_ref(), work)?
                    .is_eq()
                {
                    continue;
                }
            }
            return Ok(Some(record));
        }
        Ok(None)
    }
}
