use super::storage::FrozenStorage;
use super::{
    ArrayRows, CheckedLiteralRecord, CoordinateAxes, FinalGroupRows, FrozenLiteralBuffers,
    GeometryErrorKind, LiteralAddress, LiteralCheck, LiteralReceiver, LiteralRowAddress,
    LiteralRowBinding, LogicalRow, Point, ResourceError, Result, SealedOutput, ValidityMask, Work,
    validate_shape_layout, visit_shape_literals, visit_storage_row,
};

fn validate_group_storage(
    values: &ArrayRows,
    offsets: &[i64],
    missing: Option<&ValidityMask>,
    work: &mut impl Work,
) -> Result<usize> {
    let group_count = offsets
        .len()
        .checked_sub(1)
        .ok_or_else(|| GeometryErrorKind::Invalid("group offsets are empty".into()))?;
    if offsets.first() != Some(&0)
        || usize::try_from(*offsets.last().expect("offsets contain a sentinel")).ok()
            != Some(values.len())
        || missing.is_some_and(|missing| missing.len() != group_count)
    {
        return Err(GeometryErrorKind::Invalid(
            "group offsets or validity do not match the admitted values".into(),
        )
        .into());
    }
    work.charge_into_error(u64::try_from(offsets.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    for (group, (&start, &end)) in offsets.iter().zip(offsets.iter().skip(1)).enumerate() {
        work.charge_into_error(1)?;
        if start < 0 || end < start {
            return Err(GeometryErrorKind::Invalid(
                "group offsets are negative or decreasing".into(),
            )
            .into());
        }
        if missing.is_some_and(|missing| missing.is_missing(group)) && end != start {
            return Err(GeometryErrorKind::Invalid(
                "a missing group has a nonempty values window".into(),
            )
            .into());
        }
    }
    work.charge_into_error(1)?;
    Ok(group_count)
}

fn validate_group_literal_map<I>(
    admitted_values: &ArrayRows,
    values: &[Option<LiteralRowBinding<I>>],
    group_count: usize,
    offsets: &[i64],
    work: &mut impl Work,
) -> Result<()> {
    work.charge_into_error(u64::try_from(values.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    for (group, (&start, &end)) in offsets
        .iter()
        .zip(offsets.iter().skip(1))
        .take(group_count)
        .enumerate()
    {
        let start = usize::try_from(start)
            .map_err(|_| GeometryErrorKind::Invalid("group start offset is negative".into()))?;
        let end = usize::try_from(end)
            .map_err(|_| GeometryErrorKind::Invalid("group end offset is negative".into()))?;
        for value in start..end {
            work.charge_into_error(1)?;
            let binding = values.get(value).and_then(Option::as_ref);
            match (admitted_values.logical_row(value), binding) {
                (Some(LogicalRow::Missing { .. }), None)
                | (Some(LogicalRow::Present { .. }), Some(LiteralRowBinding::OtherProducer)) => {},
                (Some(LogicalRow::Missing { .. }), Some(_)) => {
                    return Err(GeometryErrorKind::Invalid(
                        "missing group value has a literal row map".into(),
                    )
                    .into());
                },
                (Some(LogicalRow::Present { .. }), Some(LiteralRowBinding::Checked(row))) => {
                    if row.group_row != Some(group) || row.value_row != Some(value) {
                        return Err(GeometryErrorKind::Invalid(
                            "group literal map lost its group or value row identity".into(),
                        )
                        .into());
                    }
                },
                (Some(LogicalRow::Present { .. }), None) => {
                    return Err(GeometryErrorKind::Invalid(
                        "present group value has no literal row map".into(),
                    )
                    .into());
                },
                (None, _) => {
                    return Err(GeometryErrorKind::Invalid(
                        "group value has no admitted logical row".into(),
                    )
                    .into());
                },
            }
        }
    }
    Ok(())
}

pub(in crate::geometry) fn bind_literals<I, R, W>(
    buffers: FrozenLiteralBuffers<I>,
    receiver: &mut R,
    work: &mut W,
) -> Result<LiteralCheck<I>>
where
    I: Copy + Eq + Ord,
    R: LiteralReceiver<I>,
    W: Work,
{
    bind_literals_with_record_policy(buffers, receiver, work, true)
}

pub(in crate::geometry) fn bind_final_literals<I, R, W>(
    buffers: FrozenLiteralBuffers<I>,
    receiver: &mut R,
    work: &mut W,
) -> Result<LiteralCheck<I>>
where
    I: Copy + Eq + Ord,
    R: LiteralReceiver<I>,
    W: Work,
{
    bind_literals_with_record_policy(buffers, receiver, work, false)
}

fn bind_literals_with_record_policy<I, R, W>(
    buffers: FrozenLiteralBuffers<I>,
    receiver: &mut R,
    work: &mut W,
    retain_checked: bool,
) -> Result<LiteralCheck<I>>
where
    I: Copy + Eq + Ord,
    R: LiteralReceiver<I>,
    W: Work,
{
    let mut rejected = Vec::new();
    let mut checked = Vec::new();
    let mut check =
        |address, image, record, expected_axes, stored_axes, work: &mut W| -> Result<()> {
            work.charge_into_error(1)?;
            if expected_axes != stored_axes || CoordinateAxes::from_point(record) != stored_axes {
                rejected.push(image);
                return Ok(());
            }
            let Some(actual) =
                receiver.read_point(address, image, record, expected_axes, stored_axes, work)?
            else {
                rejected.push(image);
                return Ok(());
            };
            if actual != *receiver.image(address, image)? {
                rejected.push(image);
            } else if retain_checked {
                checked.push(CheckedLiteralRecord {
                    address,
                    image,
                    record,
                    chart: actual,
                });
            }
            Ok(())
        };
    match &buffers.storage {
        FrozenStorage::Scalar { shape, layout } => {
            validate_shape_layout(layout, work)?;
            visit_shape_literals(shape, layout, LiteralRowAddress::scalar(), work, &mut check)?;
        },
        FrozenStorage::Rows {
            rows,
            row_map,
            visit_rows,
        } => visit_array_literals(rows, row_map, visit_rows, work, &mut check)?,
        FrozenStorage::Groups { groups, row_map } => {
            visit_group_literals(groups, row_map, work, &mut check)?;
        },
    }
    if rejected.is_empty() {
        Ok(LiteralCheck::Pass(SealedOutput { buffers, checked }))
    } else {
        rejected.sort_unstable();
        rejected.dedup();
        Ok(LiteralCheck::Reject(rejected))
    }
}

fn visit_array_literals<I: Copy, W: Work>(
    rows: &ArrayRows,
    row_map: &[Option<LiteralRowBinding<I>>],
    visit_rows: &std::ops::Range<usize>,
    work: &mut W,
    visit: &mut impl FnMut(
        LiteralAddress,
        I,
        Point,
        CoordinateAxes,
        CoordinateAxes,
        &mut W,
    ) -> Result<()>,
) -> Result<()> {
    if row_map.len() != rows.len()
        || visit_rows.start > visit_rows.end
        || visit_rows.end > rows.len()
    {
        return Err(GeometryErrorKind::Invalid(
            "final row storage, literal map, or visit window is invalid".into(),
        )
        .into());
    }
    work.charge_into_error(
        u64::try_from(visit_rows.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    for logical in visit_rows.clone() {
        work.charge_into_error(1)?;
        match rows.logical_row(logical).ok_or_else(|| {
            GeometryErrorKind::Invalid("final storage has no admitted logical row".into())
        })? {
            LogicalRow::Missing { .. } => {
                if row_map.get(logical).is_some_and(Option::is_some) {
                    return Err(GeometryErrorKind::Invalid(
                        "missing final row has a literal receiver map".into(),
                    )
                    .into());
                }
            },
            LogicalRow::Present { row, .. } => {
                let binding = row_map
                    .get(logical)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "present final row has no literal receiver map".into(),
                        )
                    })?;
                let LiteralRowBinding::Checked(locations) = binding else {
                    continue;
                };
                if locations.logical_row != logical
                    || locations.group_row.is_some()
                    || locations.value_row.is_some()
                {
                    return Err(GeometryErrorKind::Invalid(
                        "final row literal address does not match its logical row".into(),
                    )
                    .into());
                }
                validate_shape_layout(&locations.layout, work)?;
                visit_storage_row(
                    row,
                    &locations.layout,
                    LiteralRowAddress {
                        operation: locations.operation,
                        logical_row: locations.logical_row,
                        group_row: None,
                        value_row: None,
                    },
                    work,
                    visit,
                )?;
            },
        }
    }
    Ok(())
}

fn visit_group_literals<I: Copy, W: Work>(
    groups: &FinalGroupRows,
    row_map: &[Option<LiteralRowBinding<I>>],
    work: &mut W,
    visit: &mut impl FnMut(
        LiteralAddress,
        I,
        Point,
        CoordinateAxes,
        CoordinateAxes,
        &mut W,
    ) -> Result<()>,
) -> Result<()> {
    let group_count = validate_group_storage(
        &groups.values,
        &groups.offsets,
        groups.missing.as_ref(),
        work,
    )?;
    validate_group_literal_map(&groups.values, row_map, group_count, &groups.offsets, work)?;
    for (group, (&start, &end)) in groups
        .offsets
        .iter()
        .zip(groups.offsets.iter().skip(1))
        .take(group_count)
        .enumerate()
    {
        work.charge_into_error(1)?;
        if groups
            .missing
            .as_ref()
            .is_some_and(|missing| missing.is_missing(group))
        {
            continue;
        }
        let start = usize::try_from(start)
            .map_err(|_| GeometryErrorKind::Invalid("group start offset is negative".into()))?;
        let end = usize::try_from(end)
            .map_err(|_| GeometryErrorKind::Invalid("group end offset is negative".into()))?;
        for value in start..end {
            work.charge_into_error(1)?;
            match groups.values.logical_row(value).ok_or_else(|| {
                GeometryErrorKind::Invalid("group value has no admitted logical row".into())
            })? {
                LogicalRow::Missing { .. } => {
                    if row_map.get(value).is_some_and(Option::is_some) {
                        return Err(GeometryErrorKind::Invalid(
                            "missing group value has a literal receiver map".into(),
                        )
                        .into());
                    }
                },
                LogicalRow::Present { row, .. } => {
                    let binding = row_map.get(value).and_then(Option::as_ref).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "present group value has no literal receiver map".into(),
                        )
                    })?;
                    let LiteralRowBinding::Checked(locations) = binding else {
                        continue;
                    };
                    validate_shape_layout(&locations.layout, work)?;
                    visit_storage_row(
                        row,
                        &locations.layout,
                        LiteralRowAddress {
                            operation: locations.operation,
                            logical_row: locations.logical_row,
                            group_row: locations.group_row,
                            value_row: locations.value_row,
                        },
                        work,
                        visit,
                    )?;
                },
            }
        }
    }
    Ok(())
}
