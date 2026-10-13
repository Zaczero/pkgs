use super::super::{
    ExactChartPoint, ExactDyadic, ExactGeographicQ, ExactRational, GeometryErrorKind,
    NativeReferencePoints, Ordering, QDartId, QNodeId, QPortId, QProducerSourceRange,
    ResourceError, Result, rotation_row_matches, try_sort_by_with_work, vector_cross_with_work,
    vector_difference_with_work,
};
use crate::geographic::retained_exact::sites::retained_exact_native_site_with_work;
use crate::geometry::graph::compare_departures_with_work;
use crate::geometry::numerics::product::ExactSign;
use crate::work::Work;

pub(in crate::geometry::topology::q_embedding) fn strict_native_bank_event(
    axis: crate::geographic::PeriodicAxis,
    start: &ExactRational,
    end: &ExactRational,
    budget: &mut impl Work,
) -> Result<bool> {
    let direction = start.try_cmp_with_work(end, budget)?;
    if direction.is_eq() {
        return Ok(false);
    }
    budget.charge_into_error(1)?;
    let start_site = retained_exact_native_site_with_work(axis, start, budget)?;
    let period = ExactRational::from_f64(axis.period());
    let half = ExactRational::from_f64(axis.half_period());
    let periods = start_site.periods();
    let bank_index = if direction.is_lt() {
        periods
    } else {
        let immediately_lower = periods.checked_sub(1).ok_or(ResourceError::SizeOverflow)?;
        let lower_bank = half.try_add_with_work(
            &period.try_product_with_work(
                &ExactRational::from_dyadic(ExactDyadic::from_i64(immediately_lower)),
                budget,
            )?,
            budget,
        )?;
        if start.try_cmp_with_work(&lower_bank, budget)?.is_eq() {
            periods.checked_sub(2).ok_or(ResourceError::SizeOverflow)?
        } else {
            immediately_lower
        }
    };
    let bank = half.try_add_with_work(
        &period.try_product_with_work(
            &ExactRational::from_dyadic(ExactDyadic::from_i64(bank_index)),
            budget,
        )?,
        budget,
    )?;
    Ok(if direction.is_lt() {
        start.try_cmp_with_work(&bank, budget)?.is_lt()
            && bank.try_cmp_with_work(end, budget)?.is_lt()
    } else {
        end.try_cmp_with_work(&bank, budget)?.is_lt()
            && bank.try_cmp_with_work(start, budget)?.is_lt()
    })
}
pub(in crate::geometry::topology::q_embedding) fn producer_range_touches_node(
    range: &QProducerSourceRange,
    producer_parameter: &ExactRational,
    node: QNodeId,
    work: &mut impl Work,
) -> Result<bool> {
    let [start_node, end_node] = range.nodes;
    let at_start = range
        .producer_range
        .start
        .try_cmp_with_work(producer_parameter, work)?
        .is_eq()
        && ((range.span_range.start.is_zero() && start_node == node)
            || (range.span_range.start.is_one() && end_node == node));
    let at_end = range
        .producer_range
        .end
        .try_cmp_with_work(producer_parameter, work)?
        .is_eq()
        && ((range.span_range.end.is_zero() && start_node == node)
            || (range.span_range.end.is_one() && end_node == node));
    Ok(at_start || at_end)
}
pub(in crate::geometry::topology::q_embedding) fn producer_range_fan_dart(
    topology: &ExactGeographicQ,
    range: &QProducerSourceRange,
    producer_parameter: &ExactRational,
    node: QNodeId,
    work: &mut impl Work,
) -> Result<Option<(QPortId, QDartId)>> {
    let span = topology.spans.get(range.span).ok_or_else(|| {
        GeometryErrorKind::Invalid("producer fan range references an unknown Q span".into())
    })?;
    if span.nodes != range.nodes || span.ports != range.ports {
        return Err(GeometryErrorKind::Invalid(
            "producer fan range changed its Q-span endpoints".into(),
        )
        .into());
    }
    let forward = topology
        .span_darts
        .get(range.span)
        .copied()
        .flatten()
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("producer fan span has no directed Q dart".into())
        })?;
    let mut departure = None;
    for (producer, parameter) in [
        (&range.producer_range.start, &range.span_range.start),
        (&range.producer_range.end, &range.span_range.end),
    ] {
        if !producer
            .try_cmp_with_work(producer_parameter, work)?
            .is_eq()
        {
            continue;
        }
        let candidate = if parameter.is_zero() && range.nodes[0] == node {
            Some((range.ports[0], forward))
        } else if parameter.is_one() && range.nodes[1] == node {
            Some((range.ports[1], QDartId(forward.0 ^ 1)))
        } else {
            None
        };
        if let Some(candidate) = candidate {
            if departure.is_some_and(|previous| previous != candidate) {
                return Err(GeometryErrorKind::Invalid(
                    "producer fan endpoint maps to multiple source darts".into(),
                )
                .into());
            }
            departure = Some(candidate);
        }
    }
    if let Some((port, dart)) = departure {
        let source_dart = topology.darts.get(dart.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("producer fan source dart is unknown".into())
        })?;
        if source_dart.from_port != port {
            return Err(GeometryErrorKind::Invalid(
                "producer fan dart starts at another source port".into(),
            )
            .into());
        }
    }
    Ok(departure)
}
pub(in crate::geometry::topology::q_embedding) fn exact_departures_share_forward_ray(
    origin: &ExactChartPoint,
    left: &ExactChartPoint,
    right: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<bool> {
    work.charge_into_error(4)?;
    let left = vector_difference_with_work(origin, left, work)?;
    let right = vector_difference_with_work(origin, right, work)?;
    if !vector_cross_with_work(&left, &right, work)?.is_zero() {
        return Ok(false);
    }
    Ok(left
        .0
        .try_product_with_work(&right.0, work)?
        .try_add_with_work(&left.1.try_product_with_work(&right.1, work)?, work)?
        .sign()
        == ExactSign::Positive)
}
pub(in crate::geometry::topology::q_embedding) fn assigned_producer_fan_order_matches(
    points: &NativeReferencePoints,
    origin: &ExactChartPoint,
    rays: &mut [(QDartId, u32, ExactChartPoint)],
    expected: &[QDartId],
    work: &mut impl Work,
) -> Result<bool> {
    try_sort_by_with_work(rays, work, |left, right, work| {
        if exact_departures_share_forward_ray(origin, &left.2, &right.2, work)? {
            Ok(Ordering::Equal)
        } else {
            compare_departures_with_work(points, 0, left.1, right.1, work)
        }
    })?;
    for pair in rays.array_windows::<2>() {
        work.charge_into_error(1)?;
        if pair[0].0 != pair[1].0
            && exact_departures_share_forward_ray(origin, &pair[0].2, &pair[1].2, work)?
        {
            return Ok(false);
        }
    }
    work.charge_into_error(
        u64::try_from(
            rays.len()
                .checked_add(expected.len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let actual: Vec<_> = rays.iter().map(|(dart, ..)| *dart).collect();
    Ok(rotation_row_matches(expected, &actual))
}
