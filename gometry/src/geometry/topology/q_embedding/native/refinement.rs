use super::super::{
    ChartSpanId, ExactChartPoint, ExactDyadic, ExactQuotientKey, ExactRational, ExactSourceRange,
    GeographicFrame, GeometryErrorKind, NativeAreaEdge, NativeAreaEdgeKind,
    NativeKnownSpanBoundary, NativePortData, NativeSourceSpanUse, NativeSplitPoint, PortSpelling,
    ResourceError, Result, Work, compare_quotient_keys_with_work, interpolate_producer_parameter,
    merge_native_port_data, native_chart_vertex_key, quotient_key_for_chart_position_with_work,
    split_rational_native_site_with_work, try_sort_by_with_work,
};

pub(in crate::geometry::topology::q_embedding) fn native_events_for_span(
    events: &[NativeKnownSpanBoundary],
    span: ChartSpanId,
    budget: &mut impl Work,
) -> Result<(usize, usize)> {
    let mut low = 0_usize;
    let mut high = events.len();
    while low < high {
        budget.charge_into_error(1)?;
        let middle = low.midpoint(high);
        let event = events.get(middle).ok_or_else(|| {
            GeometryErrorKind::Invalid("native span-event index is unknown".into())
        })?;
        if event.span < span {
            low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        } else {
            high = middle;
        }
    }
    let start = low;
    high = events.len();
    while low < high {
        budget.charge_into_error(1)?;
        let middle = low.midpoint(high);
        let event = events.get(middle).ok_or_else(|| {
            GeometryErrorKind::Invalid("native span-event index is unknown".into())
        })?;
        if event.span <= span {
            low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        } else {
            high = middle;
        }
    }
    Ok((start, low))
}
pub(in crate::geometry::topology::q_embedding) fn native_interpolate_chart_point(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    parameter: &ExactRational,
    budget: &mut impl Work,
) -> Result<ExactChartPoint> {
    let dx = end.x.try_subtract_with_work(&start.x, budget)?;
    let dy = end.y.try_subtract_with_work(&start.y, budget)?;
    Ok(ExactChartPoint::from_rationals(
        start
            .x
            .try_add_with_work(&dx.try_product_with_work(parameter, budget)?, budget)?,
        start
            .y
            .try_add_with_work(&dy.try_product_with_work(parameter, budget)?, budget)?,
    ))
}
pub(in crate::geometry::topology::q_embedding) fn clip_native_source_span_use(
    source: &NativeSourceSpanUse,
    producer_start: &ExactRational,
    producer_end: &ExactRational,
    budget: &mut impl Work,
) -> Result<Option<NativeSourceSpanUse>> {
    let (low, high) = if source
        .producer_range
        .start
        .try_cmp_with_work(&source.producer_range.end, budget)?
        .is_le()
    {
        (&source.producer_range.start, &source.producer_range.end)
    } else {
        (&source.producer_range.end, &source.producer_range.start)
    };
    let overlap_start = if low.try_cmp_with_work(producer_start, budget)?.is_gt() {
        low.try_clone_with_work(budget)?
    } else {
        producer_start.try_clone_with_work(budget)?
    };
    let overlap_end = if high.try_cmp_with_work(producer_end, budget)?.is_lt() {
        high.try_clone_with_work(budget)?
    } else {
        producer_end.try_clone_with_work(budget)?
    };
    if overlap_start
        .try_cmp_with_work(&overlap_end, budget)?
        .is_gt()
    {
        return Ok(None);
    }
    let source_start = interpolate_producer_parameter(
        &source.source_range.start,
        &source.source_range.end,
        &source.producer_range.start,
        &source.producer_range.end,
        &overlap_start,
        budget,
    )?;
    let source_end = interpolate_producer_parameter(
        &source.source_range.start,
        &source.source_range.end,
        &source.producer_range.start,
        &source.producer_range.end,
        &overlap_end,
        budget,
    )?;
    let span_start = interpolate_producer_parameter(
        &source.span_range.start,
        &source.span_range.end,
        &source.producer_range.start,
        &source.producer_range.end,
        &overlap_start,
        budget,
    )?;
    let span_end = interpolate_producer_parameter(
        &source.span_range.start,
        &source.span_range.end,
        &source.producer_range.start,
        &source.producer_range.end,
        &overlap_end,
        budget,
    )?;
    Ok(Some(NativeSourceSpanUse {
        span: source.span,
        owner: source.owner,
        source_edge: source.source_edge,
        source_lift: source.source_lift,
        source_range: ExactSourceRange {
            start: source_start,
            end: source_end,
        },
        producer_range: ExactSourceRange {
            start: overlap_start,
            end: overlap_end,
        },
        span_range: ExactSourceRange {
            start: span_start,
            end: span_end,
        },
        nodes: source.nodes,
        ports: source.ports,
        face_germs: source.face_germs,
    }))
}
pub(in crate::geometry::topology::q_embedding) fn native_port_spelling_at_assigned_point(
    point: &ExactChartPoint,
    frame: GeographicFrame,
    work: &mut impl Work,
) -> Result<Option<PortSpelling>> {
    match quotient_key_for_chart_position_with_work(frame, point, work)? {
        ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole => {
            let longitude = point.x.round_with_work(work)?;
            if ExactRational::from_f64(longitude)
                .try_cmp_with_work(&point.x, work)?
                .is_eq()
            {
                Ok(Some(PortSpelling::Pole {
                    native_longitude: longitude,
                }))
            } else {
                Ok(None)
            }
        },
        ExactQuotientKey::Seam(_) => {
            let half = ExactRational::from_f64(frame.longitude().half_period());
            let west = ExactRational::zero().try_subtract_with_work(&half, work)?;
            if point.x.try_cmp_with_work(&half, work)?.is_eq() {
                Ok(Some(PortSpelling::Seam(
                    super::super::super::emission::ChartSide::East,
                )))
            } else if point.x.try_cmp_with_work(&west, work)?.is_eq() {
                Ok(Some(PortSpelling::Seam(
                    super::super::super::emission::ChartSide::West,
                )))
            } else {
                Ok(None)
            }
        },
        ExactQuotientKey::Coordinate(_) => {
            let (_, lift) = split_rational_native_site_with_work(
                &point.x,
                frame.longitude().period(),
                frame.longitude().half_period(),
                work,
            )?;
            let Some(periods) = lift.to_f64_exact_scaled(0) else {
                return Ok(None);
            };
            if !periods.is_finite()
                || periods.fract() != 0.0
                || periods < i64::MIN as f64
                || periods >= -(i64::MIN as f64)
            {
                return Ok(None);
            }
            Ok(Some(PortSpelling::Interior {
                periods: periods as i64,
            }))
        },
    }
}
pub(in crate::geometry::topology::q_embedding) fn source_lifted_native_port(
    port: &NativePortData,
    source_lift: i64,
    assigned_point: &ExactChartPoint,
    frame: GeographicFrame,
    work: &mut impl Work,
) -> Result<Option<NativePortData>> {
    let period = ExactRational::from_f64(frame.longitude().period());
    let lift = ExactRational::from_dyadic(ExactDyadic::from_i64(source_lift));
    let lifted_x = port
        .point
        .x
        .try_add_with_work(&lift.try_product_with_work(&period, work)?, work)?;
    let lifted_y = port.point.y.try_clone_with_work(work)?;
    if !lifted_y.try_cmp_with_work(&assigned_point.y, work)?.is_eq() {
        return Ok(None);
    }
    let lifted_point = ExactChartPoint::from_rationals(lifted_x, lifted_y);
    let source_key = quotient_key_for_chart_position_with_work(frame, &lifted_point, work)?;
    let assigned_key = quotient_key_for_chart_position_with_work(frame, assigned_point, work)?;
    if !compare_quotient_keys_with_work(&source_key, &assigned_key, work)?.is_eq() {
        return Ok(None);
    }
    let turns = assigned_point
        .x
        .try_subtract_with_work(&lifted_point.x, work)?
        .try_quotient_with_work(&period, work)?;
    if !turns.is_integer() {
        return Ok(None);
    }
    let aligned_x = lifted_point
        .x
        .try_add_with_work(&period.try_product_with_work(&turns, work)?, work)?;
    if !aligned_x
        .try_cmp_with_work(&assigned_point.x, work)?
        .is_eq()
    {
        return Ok(None);
    }
    let point = ExactChartPoint::from_rationals(aligned_x, lifted_point.y);
    let spelling = if point.try_eq_with_work(&port.point, work)? {
        port.spelling.clone()
    } else {
        let Some(spelling) = native_port_spelling_at_assigned_point(&point, frame, work)? else {
            return Ok(None);
        };
        spelling
    };
    let mut shifted = port.try_clone_with_work(work)?;
    shifted.point = point;
    shifted.spelling = spelling;
    Ok(Some(shifted))
}
pub(in crate::geometry::topology::q_embedding) fn common_refine_native_edges(
    area_edges: Vec<NativeAreaEdge>,
    boundaries: &[NativeKnownSpanBoundary],
    frame: GeographicFrame,
    budget: &mut impl Work,
) -> Result<Vec<NativeAreaEdge>> {
    budget.charge_into_error(
        u64::try_from(area_edges.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut refined = Vec::with_capacity(area_edges.len());
    for edge in area_edges {
        budget.charge_into_error(1)?;
        let NativeAreaEdge {
            vertices,
            owner,
            kind,
        } = edge;
        let NativeAreaEdgeKind::Producer {
            recipe,
            trace_edge,
            producer_start,
            producer_end,
            trace_ranges,
            known_spans,
        } = kind
        else {
            budget.charge_into_error(1)?;
            refined.push(NativeAreaEdge {
                vertices,
                owner,
                kind,
            });
            continue;
        };
        if !producer_start
            .try_cmp_with_work(&producer_end, budget)?
            .is_lt()
        {
            return Err(GeometryErrorKind::Invalid(
                "native producer edge has an empty parameter interval".into(),
            )
            .into());
        }
        budget.charge_into_error(2)?;
        let mut split_points = Vec::with_capacity(2);
        split_points.push(NativeSplitPoint {
            parameter: producer_start.try_clone_with_work(budget)?,
            data: vertices[0].try_clone_with_work(budget)?,
        });
        split_points.push(NativeSplitPoint {
            parameter: producer_end.try_clone_with_work(budget)?,
            data: vertices[1].try_clone_with_work(budget)?,
        });
        for source in &known_spans {
            let (event_start, event_end) = native_events_for_span(boundaries, source.span, budget)?;
            let (span_low, span_high) = if source
                .span_range
                .start
                .try_cmp_with_work(&source.span_range.end, budget)?
                .is_le()
            {
                (&source.span_range.start, &source.span_range.end)
            } else {
                (&source.span_range.end, &source.span_range.start)
            };
            for event_index in event_start..event_end {
                budget.charge_into_error(1)?;
                let event = boundaries.get(event_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native span event is unknown".into())
                })?;
                if event.parameter.try_cmp_with_work(span_low, budget)?.is_lt()
                    || event
                        .parameter
                        .try_cmp_with_work(span_high, budget)?
                        .is_gt()
                {
                    continue;
                }
                let expected_port = if event.parameter.is_zero() {
                    source.ports.first().copied().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native Q-span incidence has no first endpoint port".into(),
                        )
                    })?
                } else if event.parameter.is_one() {
                    source.ports.get(1).copied().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native Q-span incidence has no second endpoint port".into(),
                        )
                    })?
                } else {
                    return Err(GeometryErrorKind::Invalid(
                        "native known-span event is not an endpoint Q port".into(),
                    )
                    .into());
                };
                if event.port != expected_port {
                    return Err(GeometryErrorKind::Invalid(
                        "native known-span event changed its endpoint Q-port identity".into(),
                    )
                    .into());
                }
                let parameter = interpolate_producer_parameter(
                    &source.producer_range.start,
                    &source.producer_range.end,
                    &source.span_range.start,
                    &source.span_range.end,
                    &event.parameter,
                    budget,
                )?;
                if parameter
                    .try_cmp_with_work(&producer_start, budget)?
                    .is_le()
                    || parameter.try_cmp_with_work(&producer_end, budget)?.is_ge()
                {
                    continue;
                }
                let local = parameter
                    .try_subtract_with_work(&producer_start, budget)?
                    .try_quotient_with_work(
                        &producer_end.try_subtract_with_work(&producer_start, budget)?,
                        budget,
                    )?;
                let point = native_interpolate_chart_point(
                    &vertices[0].point,
                    &vertices[1].point,
                    &local,
                    budget,
                )?;
                let Some(data) = source_lifted_native_port(
                    &event.data,
                    source.source_lift,
                    &point,
                    frame,
                    budget,
                )?
                else {
                    return Err(GeometryErrorKind::NativeRefinementMismatch.into());
                };
                if !point.try_eq_with_work(&data.point, budget)? {
                    return Err(GeometryErrorKind::NativeRefinementMismatch.into());
                }
                budget.charge_into_error(1)?;
                split_points.push(NativeSplitPoint { parameter, data });
            }
        }
        try_sort_by_with_work(&mut split_points, budget, |left, right, work| {
            work.charge_into_error(1)?;
            left.parameter.try_cmp_with_work(&right.parameter, work)
        })?;
        budget.charge_into_error(
            u64::try_from(split_points.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut unique: Vec<NativeSplitPoint> = Vec::with_capacity(split_points.len());
        for point in split_points {
            budget.charge_into_error(1)?;
            if let Some(previous) = unique.last_mut()
                && previous
                    .parameter
                    .try_cmp_with_work(&point.parameter, budget)?
                    .is_eq()
            {
                if native_chart_vertex_key(&previous.data) != native_chart_vertex_key(&point.data)
                    || previous.data.point != point.data.point
                {
                    return Err(GeometryErrorKind::Invalid(
                        "native common-refinement event has conflicting provider identities".into(),
                    )
                    .into());
                }
                previous.data = merge_native_port_data(frame, &previous.data, &point.data, budget)?;
                continue;
            }
            unique.push(point);
        }
        if unique.len() == 2 {
            budget.charge_into_error(1)?;
            refined.push(NativeAreaEdge {
                vertices,
                owner,
                kind: NativeAreaEdgeKind::Producer {
                    recipe,
                    trace_edge,
                    producer_start,
                    producer_end,
                    trace_ranges,
                    known_spans,
                },
            });
            continue;
        }
        for adjacent in unique.array_windows::<2>() {
            let [start, end] = adjacent;
            if !start
                .parameter
                .try_cmp_with_work(&end.parameter, budget)?
                .is_lt()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native common-refinement events are not strictly ordered".into(),
                )
                .into());
            }
            let local_start = start
                .parameter
                .try_subtract_with_work(&producer_start, budget)?
                .try_quotient_with_work(
                    &producer_end.try_subtract_with_work(&producer_start, budget)?,
                    budget,
                )?;
            let local_end = end
                .parameter
                .try_subtract_with_work(&producer_start, budget)?
                .try_quotient_with_work(
                    &producer_end.try_subtract_with_work(&producer_start, budget)?,
                    budget,
                )?;
            budget.charge_into_error(
                u64::try_from(trace_ranges.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut child_ranges = Vec::with_capacity(trace_ranges.len());
            for (owner, range) in &trace_ranges {
                budget.charge_into_error(1)?;
                if let Some(range) = range.compose_with_work(&local_start, &local_end, budget)? {
                    child_ranges.push((*owner, range));
                }
            }
            budget.charge_into_error(
                u64::try_from(known_spans.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut child_spans = Vec::with_capacity(known_spans.len());
            for known in &known_spans {
                budget.charge_into_error(1)?;
                if let Some(known) =
                    clip_native_source_span_use(known, &start.parameter, &end.parameter, budget)?
                {
                    child_spans.push(known);
                }
            }
            budget.charge_into_error(1)?;
            refined.push(NativeAreaEdge {
                vertices: [
                    start.data.try_clone_with_work(budget)?,
                    end.data.try_clone_with_work(budget)?,
                ],
                owner,
                kind: NativeAreaEdgeKind::Producer {
                    recipe,
                    trace_edge,
                    producer_start: Box::new(start.parameter.try_clone_with_work(budget)?),
                    producer_end: end.parameter.try_clone_with_work(budget)?,
                    trace_ranges: child_ranges,
                    known_spans: child_spans,
                },
            });
        }
    }
    Ok(refined)
}
