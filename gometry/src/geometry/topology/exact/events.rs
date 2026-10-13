use std::cmp::Ordering;

use super::{
    CutEvent, ExactChartPiece, ExactChartPoint, FallibleAvl, in_unit_interval,
    interpolate_exact_segment, parameter_on_exact_segment, point_on_exact_segment, vector_cross,
    vector_difference,
};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;
use crate::geometry::tessellation::exact::ExactRational;
use crate::work::Work;

pub(in crate::geometry::topology::exact) fn compare_cut_events(
    left: &CutEvent,
    right: &CutEvent,
) -> Result<Ordering> {
    let piece = left.piece.cmp(&right.piece);
    Ok(if piece.is_eq() {
        left.parameter.try_cmp(&right.parameter)?
    } else {
        piece
    })
}

pub(in crate::geometry::topology::exact) fn insert_cut_event<W: Work>(
    cuts: &mut FallibleAvl<CutEvent>,
    piece: &ExactChartPiece,
    parameter: ExactRational,
    point: ExactChartPoint,
    work: &mut W,
    compare: &mut impl FnMut(&CutEvent, &CutEvent, &mut W) -> Result<Ordering>,
) -> Result<()> {
    work.charge_into_error(1)?;
    if parameter.is_zero() {
        work.charge_into_error(1)?;
        if !point.same_position(&piece.start)? {
            return Err(GeometryErrorKind::Invalid(
                "an exact source-start parameter produced a different chart point".into(),
            )
            .into());
        }
        return Ok(());
    }
    if parameter.is_one() {
        work.charge_into_error(1)?;
        if !point.same_position(&piece.end)? {
            return Err(GeometryErrorKind::Invalid(
                "an exact source-end parameter produced a different chart point".into(),
            )
            .into());
        }
        return Ok(());
    }
    let event = CutEvent {
        piece: piece.id,
        parameter,
        point,
    };
    work.charge_into_error(1)?;
    if !cuts.insert_with_work(event.clone(), work, compare)?
        && !cuts
            .get_with_work(&event, work, compare)?
            .expect("a duplicate exact cut remains in its owner tree")
            .point
            .same_position(&event.point)?
    {
        return Err(GeometryErrorKind::Invalid(
            "one exact chart parameter produced two contact points".into(),
        )
        .into());
    }
    Ok(())
}

pub(in crate::geometry::topology::exact) fn exact_piece_contacts<W: Work>(
    left: &ExactChartPiece,
    right: &ExactChartPiece,
    work: &mut W,
    mut contact: impl FnMut(ExactChartPoint, ExactRational, ExactRational, &mut W) -> Result<()>,
) -> Result<()> {
    work.charge_into_error(1)?;
    let r = vector_difference(&left.start, &left.end)?;
    work.charge_into_error(1)?;
    let s = vector_difference(&right.start, &right.end)?;
    work.charge_into_error(1)?;
    let q_minus_p = vector_difference(&left.start, &right.start)?;
    work.charge_into_error(1)?;
    let denominator = vector_cross(&r, &s)?;
    if denominator.is_zero() {
        work.charge_into_error(1)?;
        if !vector_cross(&q_minus_p, &r)?.is_zero() {
            return Ok(());
        }
        for point in [&left.start, &left.end, &right.start, &right.end] {
            work.charge_into_error(1)?;
            if point_on_exact_segment(point, &left.start, &left.end)?
                && point_on_exact_segment(point, &right.start, &right.end)?
            {
                work.charge_into_error(2)?;
                contact(
                    (*point).clone(),
                    parameter_on_exact_segment(point, &left.start, &left.end)?,
                    parameter_on_exact_segment(point, &right.start, &right.end)?,
                    work,
                )?;
            }
        }
        return Ok(());
    }

    work.charge_into_error(1)?;
    let left_parameter = vector_cross(&q_minus_p, &s)?.try_quotient(&denominator)?;
    work.charge_into_error(1)?;
    if !in_unit_interval(&left_parameter)? {
        return Ok(());
    }
    work.charge_into_error(1)?;
    let right_parameter = vector_cross(&q_minus_p, &r)?.try_quotient(&denominator)?;
    work.charge_into_error(1)?;
    if !in_unit_interval(&right_parameter)? {
        return Ok(());
    }
    work.charge_into_error(1)?;
    let point = interpolate_exact_segment(&left.start, &left.end, &left_parameter)?;
    contact(point, left_parameter, right_parameter, work)
}
