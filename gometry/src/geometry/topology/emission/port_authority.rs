use super::{
    ChartSide, EmissionNodeId, ExactChartPoint, ExactDyadic, ExactQuotientKey, ExactRational,
    GeographicFrame, GeometryErrorKind, Latitude, OccurrenceSource, Point, PortSpelling,
    ResourceError, Result, compare_quotient_keys_with_work,
    quotient_key_for_chart_position_with_work, split_rational_native_site_with_work,
};
use crate::work::Work;

pub(in crate::geometry::topology) struct NativePortData {
    pub(in crate::geometry::topology) node: EmissionNodeId,
    pub(in crate::geometry::topology) point: ExactChartPoint,
    pub(in crate::geometry::topology) spelling: PortSpelling,
    pub(in crate::geometry::topology) source_record: Option<Point>,
    pub(in crate::geometry::topology) occurrences: Vec<OccurrenceSource>,
}

impl NativePortData {
    pub(in crate::geometry::topology) fn try_clone_with_work(
        &self,
        work: &mut impl Work,
    ) -> Result<Self> {
        work.charge_into_error(
            u64::try_from(self.occurrences.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let point = self.point.try_clone_with_work(work)?;
        let mut occurrences = Vec::with_capacity(self.occurrences.len());
        for occurrence in &self.occurrences {
            occurrences.push(occurrence.try_clone_with_work(work)?);
        }
        Ok(Self {
            node: self.node,
            point,
            spelling: self.spelling.clone(),
            source_record: self.source_record,
            occurrences,
        })
    }
}

impl OccurrenceSource {
    pub(super) fn try_clone_with_work(&self, work: &mut impl Work) -> Result<Self> {
        Ok(Self {
            origin: self.origin,
            source_trace: self
                .source_trace
                .as_ref()
                .map(|location| location.try_clone_with_work(work))
                .transpose()?,
        })
    }
}

pub(super) fn exact_chart_points_equal_with_work(
    left: &ExactChartPoint,
    right: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<bool> {
    Ok(left.x.try_cmp_with_work(&right.x, work)?.is_eq()
        && left.y.try_cmp_with_work(&right.y, work)?.is_eq())
}

pub(super) fn validate_native_port_image(
    frame: GeographicFrame,
    left: &NativePortData,
    right: &NativePortData,
    work: &mut impl Work,
) -> Result<()> {
    let left_key = quotient_key_for_chart_position_with_work(frame, &left.point, work)?;
    let right_key = quotient_key_for_chart_position_with_work(frame, &right.point, work)?;
    if !compare_quotient_keys_with_work(&left_key, &right_key, work)?.is_eq() {
        return Err(GeometryErrorKind::Invalid(
            "native output vertex aliases disagree on exact quotient image".into(),
        )
        .into());
    }
    match (&left.spelling, &right.spelling, &left_key, &right_key) {
        (
            PortSpelling::Interior {
                periods: left_periods,
            },
            PortSpelling::Interior {
                periods: right_periods,
            },
            ExactQuotientKey::Coordinate(_),
            ExactQuotientKey::Coordinate(_),
        ) => {
            let longitude = frame.longitude();
            let (left_native, left_lift) = split_rational_native_site_with_work(
                &left.point.x,
                longitude.period(),
                longitude.half_period(),
                work,
            )?;
            let (right_native, right_lift) = split_rational_native_site_with_work(
                &right.point.x,
                longitude.period(),
                longitude.half_period(),
                work,
            )?;
            if !left_native
                .as_ref()
                .try_cmp_with_work(right_native.as_ref(), work)?
                .is_eq()
                || !left_lift
                    .cmp_with_work(&ExactDyadic::from_i64(*left_periods), work)?
                    .is_eq()
                || !right_lift
                    .cmp_with_work(&ExactDyadic::from_i64(*right_periods), work)?
                    .is_eq()
                || (left_periods == right_periods
                    && !exact_chart_points_equal_with_work(&left.point, &right.point, work)?)
            {
                return Err(GeometryErrorKind::Invalid(
                    "native interior aliases disagree on their exact periodic image".into(),
                )
                .into());
            }
        },
        (
            PortSpelling::Seam(left_side),
            PortSpelling::Seam(right_side),
            ExactQuotientKey::Seam(_),
            ExactQuotientKey::Seam(_),
        ) if left_side == right_side => {
            let longitude = ExactRational::from_f64(match left_side {
                ChartSide::East => frame.longitude().half_period(),
                ChartSide::West => -frame.longitude().half_period(),
            });
            if !left.point.x.try_cmp_with_work(&longitude, work)?.is_eq()
                || !right.point.x.try_cmp_with_work(&longitude, work)?.is_eq()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native seam aliases disagree on their assigned bank image".into(),
                )
                .into());
            }
        },
        (
            PortSpelling::Seam(left_side),
            PortSpelling::Seam(right_side),
            left_pole @ (ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole),
            right_pole @ (ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole),
        ) if left_side == right_side
            && compare_quotient_keys_with_work(left_pole, right_pole, work)?.is_eq() =>
        {
            let longitude = ExactRational::from_f64(match left_side {
                ChartSide::East => frame.longitude().half_period(),
                ChartSide::West => -frame.longitude().half_period(),
            });
            if !left.point.x.try_cmp_with_work(&longitude, work)?.is_eq()
                || !right.point.x.try_cmp_with_work(&longitude, work)?.is_eq()
                || !exact_chart_points_equal_with_work(&left.point, &right.point, work)?
            {
                return Err(GeometryErrorKind::Invalid(
                    "native pole-bank aliases disagree on their exact chart occurrence".into(),
                )
                .into());
            }
        },
        (
            PortSpelling::Pole {
                native_longitude: left_longitude,
            },
            PortSpelling::Pole {
                native_longitude: right_longitude,
            },
            left_pole @ (ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole),
            right_pole @ (ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole),
        ) if compare_quotient_keys_with_work(left_pole, right_pole, work)?.is_eq() => {
            let same_spelling = left_longitude.to_bits() == right_longitude.to_bits();
            let left_longitude = ExactRational::from_f64(*left_longitude);
            let right_longitude = ExactRational::from_f64(*right_longitude);
            let (left_native, _) = split_rational_native_site_with_work(
                &left_longitude,
                frame.longitude().period(),
                frame.longitude().half_period(),
                work,
            )?;
            let (right_native, _) = split_rational_native_site_with_work(
                &right_longitude,
                frame.longitude().period(),
                frame.longitude().half_period(),
                work,
            )?;
            let (left_point_native, _) = split_rational_native_site_with_work(
                &left.point.x,
                frame.longitude().period(),
                frame.longitude().half_period(),
                work,
            )?;
            let (right_point_native, _) = split_rational_native_site_with_work(
                &right.point.x,
                frame.longitude().period(),
                frame.longitude().half_period(),
                work,
            )?;
            if !same_spelling
                || !left_native
                    .as_ref()
                    .try_cmp_with_work(left_point_native.as_ref(), work)?
                    .is_eq()
                || !right_native
                    .as_ref()
                    .try_cmp_with_work(right_point_native.as_ref(), work)?
                    .is_eq()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native pole aliases disagree on their typed identity or explicit spelling"
                        .into(),
                )
                .into());
            }
        },
        _ => {
            return Err(GeometryErrorKind::Invalid(
                "native output aliases use incompatible chart spellings".into(),
            )
            .into());
        },
    }
    Ok(())
}

pub(in crate::geometry::topology) fn native_source_record_quotient_key(
    frame: GeographicFrame,
    record: Point,
    assigned: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<ExactQuotientKey> {
    let record_point = ExactChartPoint::from_rationals(
        ExactRational::from_f64(record.x),
        ExactRational::from_f64(Latitude::from_admitted(frame, record.y).get()),
    );
    let record_key = quotient_key_for_chart_position_with_work(frame, &record_point, work)?;
    let assigned_key = quotient_key_for_chart_position_with_work(frame, assigned, work)?;
    let longitude = frame.longitude();
    let (record_native, _) = split_rational_native_site_with_work(
        &record_point.x,
        longitude.period(),
        longitude.half_period(),
        work,
    )?;
    let (assigned_native, _) = split_rational_native_site_with_work(
        &assigned.x,
        longitude.period(),
        longitude.half_period(),
        work,
    )?;
    if !compare_quotient_keys_with_work(&record_key, &assigned_key, work)?.is_eq()
        || !record_native
            .as_ref()
            .try_cmp_with_work(assigned_native.as_ref(), work)?
            .is_eq()
    {
        return Err(GeometryErrorKind::Invalid(
            "authentic native source record differs from its assigned endpoint image".into(),
        )
        .into());
    }
    Ok(record_key)
}

pub(super) fn validate_native_source_record(
    frame: GeographicFrame,
    port: &NativePortData,
    record: Point,
    work: &mut impl Work,
) -> Result<()> {
    let record_key = native_source_record_quotient_key(frame, record, &port.point, work)?;
    let longitude = frame.longitude();
    match &port.spelling {
        PortSpelling::Seam(side) => {
            let expected = match side {
                ChartSide::East => longitude.half_period(),
                ChartSide::West => -longitude.half_period(),
            };
            if record.x.to_bits() != expected.to_bits() {
                return Err(GeometryErrorKind::Invalid(
                    "authentic native source record changes its seam-bank spelling".into(),
                )
                .into());
            }
        },
        PortSpelling::Pole { native_longitude } => {
            if record.x.to_bits() != native_longitude.to_bits()
                || !matches!(
                    record_key,
                    ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole
                )
            {
                return Err(GeometryErrorKind::Invalid(
                    "authentic native pole record changes its explicit spelling".into(),
                )
                .into());
            }
        },
        PortSpelling::Interior { .. } => {
            if !matches!(record_key, ExactQuotientKey::Coordinate(_)) {
                return Err(GeometryErrorKind::Invalid(
                    "authentic native interior record is not a coordinate image".into(),
                )
                .into());
            }
        },
    }
    Ok(())
}

pub(super) fn native_source_record_spells_chart_point(
    frame: GeographicFrame,
    record: Point,
    point: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<bool> {
    let admitted_y = Latitude::from_admitted(frame, record.y).get();
    Ok(ExactRational::from_f64(record.x)
        .try_cmp_with_work(&point.x, work)?
        .is_eq()
        && ExactRational::from_f64(admitted_y)
            .try_cmp_with_work(&point.y, work)?
            .is_eq())
}

pub(in crate::geometry::topology) fn merge_native_port_data(
    frame: GeographicFrame,
    left: &NativePortData,
    right: &NativePortData,
    work: &mut impl Work,
) -> Result<NativePortData> {
    if left.node != right.node {
        return Err(GeometryErrorKind::Invalid(
            "native output vertex aliases disagree on quotient identity".into(),
        )
        .into());
    }
    validate_native_port_image(frame, left, right, work)?;
    let spelling = left.spelling.clone();
    for port in [left, right] {
        if let Some(record) = port.source_record {
            validate_native_source_record(frame, port, record, work)?;
        }
    }
    // Retain the authentic record for the chosen chart lift; ambiguous raw bits
    // are not resolved by the order in which graph contributors were visited.
    let source_record = match (left.source_record, right.source_record) {
        (Some(left), Some(right))
            if left.x.to_bits() == right.x.to_bits() && left.y.to_bits() == right.y.to_bits() =>
        {
            Some(left)
        },
        (Some(left_record), Some(right_record)) => {
            let left_spelling =
                native_source_record_spells_chart_point(frame, left_record, &left.point, work)?;
            let right_spelling =
                native_source_record_spells_chart_point(frame, right_record, &left.point, work)?;
            match (left_spelling, right_spelling) {
                (true, false) => Some(left_record),
                (false, true) => Some(right_record),
                (false, false) => None,
                (true, true) => {
                    return Err(GeometryErrorKind::Invalid(
                        "native aliases carry conflicting exact source spellings".into(),
                    )
                    .into());
                },
            }
        },
        (Some(record), None) | (None, Some(record)) => Some(record),
        (None, None) => None,
    };
    let occurrence_count = left
        .occurrences
        .len()
        .checked_add(right.occurrences.len())
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge_into_error(
        u64::try_from(occurrence_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut occurrences = Vec::with_capacity(occurrence_count);
    for occurrence in &left.occurrences {
        occurrences.push(occurrence.try_clone_with_work(work)?);
    }
    for occurrence in &right.occurrences {
        occurrences.push(occurrence.try_clone_with_work(work)?);
    }
    Ok(NativePortData {
        node: left.node,
        point: left.point.try_clone_with_work(work)?,
        spelling,
        source_record,
        occurrences,
    })
}
