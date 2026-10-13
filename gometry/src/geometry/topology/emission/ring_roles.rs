use std::cmp::Ordering;

use super::super::exact::ExactChartPoint;
use super::sites::canonical_bits;
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartPole, GeographicFrame, GeographicRing, RingSummary};
#[cfg(test)]
use crate::geometry::Unmetered;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::exact::{ExactRational, Interval};
use crate::geometry::{CoordSeq, GeometryErrorKind, Point, Ring, RingWinding, Work};

#[derive(Clone, Copy)]
pub(in crate::geometry::topology) struct QRingRole {
    pub(in crate::geometry::topology) emitted_is_hole: bool,
    pub(in crate::geometry::topology) exact_is_hole: bool,
    pub(in crate::geometry::topology) published_matches_exact: bool,
}

impl QRingRole {
    pub(in crate::geometry::topology) const fn matches(self) -> bool {
        self.emitted_is_hole == self.exact_is_hole && self.published_matches_exact
    }
}

pub(in crate::geometry::topology) fn finite_interval(value: Interval) -> Option<Interval> {
    (value.is_finite() && value.lo <= value.hi).then_some(value)
}

pub(in crate::geometry::topology) fn canonical_longitude_interval(
    frame: GeographicFrame,
    raw: Interval,
    period: Interval,
) -> Option<Interval> {
    let half = frame.longitude().half_period();
    let canonical = if raw.lo >= half {
        raw.sub(period)
    } else if raw.hi < -half {
        raw.add(period)
    } else if raw.lo >= -half && raw.hi < half {
        raw
    } else {
        return None;
    };
    finite_interval(canonical)
}

pub(in crate::geometry::topology) fn lift_longitude_interval(
    frame: GeographicFrame,
    raw: Interval,
    reference: Interval,
    period: Interval,
) -> Option<Interval> {
    let canonical = canonical_longitude_interval(frame, raw, period)?;
    let difference = finite_interval(reference.sub(canonical))?;
    let half = frame.longitude().half_period();
    if difference.hi < -half {
        finite_interval(canonical.sub(period))
    } else if difference.lo > half {
        finite_interval(canonical.add(period))
    } else if difference.lo > -half && difference.hi < half {
        Some(canonical)
    } else {
        None
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology) struct ExactRingWinding {
    pub(in crate::geometry::topology) winding: RingWinding,
    pub(in crate::geometry::topology) quotient_closure: bool,
}

pub(in crate::geometry::topology) fn exact_q_ring_winding_filter<'a, I>(
    frame: GeographicFrame,
    exact_ports: I,
) -> Option<ExactRingWinding>
where
    I: IntoIterator<Item = &'a ExactChartPoint>,
    I::IntoIter: DoubleEndedIterator,
{
    let mut interior = exact_ports.into_iter();
    let first = interior.next()?;
    interior.next_back()?;
    let (origin_x_low, origin_x_high) = first.x.outward_bounds().ok()?;
    let origin_x = finite_interval(Interval {
        lo: origin_x_low,
        hi: origin_x_high,
    })?;
    let (origin_y_low, origin_y_high) = first.y.outward_bounds().ok()?;
    let origin_y = finite_interval(Interval {
        lo: origin_y_low,
        hi: origin_y_high,
    })?;
    let period = Interval::exact(frame.longitude().period());
    let mut reference_x = origin_x;
    let mut previous_x = Interval::exact(0.0);
    let mut previous_y = Interval::exact(0.0);
    let mut twice_area = Interval::exact(0.0);

    for point in interior {
        let (raw_x_low, raw_x_high) = point.x.outward_bounds().ok()?;
        let raw_x = finite_interval(Interval {
            lo: raw_x_low,
            hi: raw_x_high,
        })?;
        let lifted_x = lift_longitude_interval(frame, raw_x, reference_x, period)?;
        let (y_low, y_high) = point.y.outward_bounds().ok()?;
        let y = finite_interval(Interval {
            lo: y_low,
            hi: y_high,
        })?;
        let x = finite_interval(lifted_x.sub(origin_x))?;
        let y = finite_interval(y.sub(origin_y))?;
        let forward = finite_interval(previous_x.mul(y))?;
        let reverse = finite_interval(x.mul(previous_y))?;
        let term = finite_interval(forward.sub(reverse))?;
        twice_area = finite_interval(twice_area.add(term))?;
        previous_x = x;
        previous_y = y;
        reference_x = lifted_x;
    }

    let closing = lift_longitude_interval(frame, origin_x, reference_x, period)?;
    let closure = finite_interval(closing.sub(origin_x))?;
    let winding = if twice_area.lo > 0.0 {
        RingWinding::CounterClockwise
    } else if twice_area.hi < 0.0 {
        RingWinding::Clockwise
    } else {
        return None;
    };
    Some(ExactRingWinding {
        winding,
        quotient_closure: closure.lo > 0.0 || closure.hi < 0.0,
    })
}

#[cfg(test)]
pub(in crate::geometry::topology) fn exact_q_ring_role<'a, I>(
    frame: GeographicFrame,
    exact_ports: I,
) -> Result<bool>
where
    I: IntoIterator<Item = &'a ExactChartPoint>,
    I::IntoIter: Clone + ExactSizeIterator + DoubleEndedIterator,
{
    let mut work = Unmetered;
    let Some((is_hole, _)) = exact_q_ring_role_with_work(frame, exact_ports, &mut work)? else {
        return Err(GeometryErrorKind::Invalid(
            "selected Q area boundary has no bounded pole".into(),
        )
        .into());
    };
    Ok(is_hole)
}

pub(in crate::geometry::topology) fn exact_q_ring_role_with_work<'a, I>(
    frame: GeographicFrame,
    exact_ports: I,
    work: &mut impl Work,
) -> Result<Option<(bool, Option<ChartPole>)>>
where
    I: IntoIterator<Item = &'a ExactChartPoint>,
    I::IntoIter: Clone + ExactSizeIterator + DoubleEndedIterator,
{
    let exact_ports = exact_ports.into_iter();
    if exact_ports.len() < 4 {
        return Err(GeometryErrorKind::Invalid(
            "Q exact ring has fewer than three vertices".into(),
        )
        .into());
    }
    let mut ends = exact_ports.clone();
    let first = ends.next().expect("length checked");
    let last = ends.next_back().expect("length checked");
    if !first.y.try_cmp_with_work(&last.y, work)?.is_eq() {
        return Err(GeometryErrorKind::Invalid(
            "Q exact ring endpoints have different latitudes".into(),
        )
        .into());
    }

    work.charge_into_error(
        u64::try_from(exact_ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    if let Some(proof) = exact_q_ring_winding_filter(frame, exact_ports.clone())
        && !proof.quotient_closure
    {
        return Ok(Some((proof.winding == RingWinding::Clockwise, None)));
    }

    let mut owned_ports = Vec::with_capacity(exact_ports.len());
    for point in exact_ports {
        work.charge_into_error(1)?;
        owned_ports.push(point.try_clone_with_work(work)?);
    }
    let exact_ports = owned_ports;
    let lifted = continuously_lift_exact_ring_with_work(frame, &exact_ports, work)?;
    let exact_published = rounded_canonical_exact_ring_with_work(frame, &exact_ports, work)?;
    match quotient_ring_role_status(frame, &exact_published)? {
        QuotientRingRoleStatus::Classified(role) => {
            return Ok(Some((role, exact_ring_bounded_pole(&exact_ports, work)?)));
        },
        QuotientRingRoleStatus::Unbounded => return Ok(None),
        QuotientRingRoleStatus::Unclassifiable => {},
    }
    let winding = exact_ring_winding_with_work(&lifted, work)?;
    match winding {
        RingWinding::CounterClockwise => {
            for point in &lifted {
                work.charge_into_error(1)?;
                point.x.round_with_work(work)?;
                point.y.round_with_work(work)?;
            }
            Ok(Some((false, None)))
        },
        RingWinding::Clockwise => {
            for point in &lifted {
                work.charge_into_error(1)?;
                point.x.round_with_work(work)?;
                point.y.round_with_work(work)?;
            }
            Ok(Some((true, None)))
        },
        RingWinding::Degenerate => {
            let exact_ring = rounded_exact_ring_with_work(&lifted, work)?;
            ring_role_from_winding(frame, winding, &exact_ring, true)
                .map(|is_hole| Some((is_hole, None)))
        },
    }
}

pub(in crate::geometry::topology) fn exact_q_ring_role_for_linework(
    frame: GeographicFrame,
    exact_ports: &[ExactChartPoint],
    work: &mut impl Work,
) -> Result<Option<(bool, Option<ChartPole>)>> {
    exact_q_ring_role_with_work(frame, exact_ports.iter(), work)
}

pub(in crate::geometry::topology) fn quotient_ring_role(
    frame: GeographicFrame,
    ring: &Ring,
) -> Result<Option<bool>> {
    match quotient_ring_role_status(frame, ring)? {
        QuotientRingRoleStatus::Unclassifiable => Ok(None),
        QuotientRingRoleStatus::Classified(role) => Ok(Some(role)),
        QuotientRingRoleStatus::Unbounded => Err(GeometryErrorKind::Invalid(
            "selected Q area boundary has no bounded pole".into(),
        )
        .into()),
    }
}

pub(in crate::geometry::topology) fn ring_role_from_winding(
    frame: GeographicFrame,
    winding: RingWinding,
    ring: &Ring,
    quotient_closure: bool,
) -> Result<bool> {
    if (quotient_closure || winding == RingWinding::Degenerate)
        && let Some(role) = quotient_ring_role(frame, ring)?
    {
        return Ok(role);
    }
    match winding {
        RingWinding::CounterClockwise => Ok(false),
        RingWinding::Clockwise => Ok(true),
        RingWinding::Degenerate => Err(GeometryErrorKind::Invalid(
            "selected Q area boundary has no chart winding".into(),
        )
        .into()),
    }
}

pub(in crate::geometry::topology) fn rounded_canonical_exact_ring_with_work(
    frame: GeographicFrame,
    points: &[ExactChartPoint],
    work: &mut impl Work,
) -> Result<Ring> {
    work.charge_into_error(u64::try_from(points.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let points = points
        .iter()
        .map(|point| {
            work.charge_into_error(1)?;
            Ok(Point::new_raw_xy(
                round_canonical_longitude_with_work(frame, &point.x, work)?,
                point.y.round_with_work(work)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ring::try_from_closed(CoordSeq::try_from_points(&points)?)
}

#[cfg(test)]
pub(in crate::geometry::topology) fn continuously_lift_exact_ring(
    frame: GeographicFrame,
    exact_ports: &[ExactChartPoint],
) -> Result<Vec<ExactChartPoint>> {
    let mut work = Unmetered;
    continuously_lift_exact_ring_with_work(frame, exact_ports, &mut work)
}

pub(in crate::geometry::topology) fn continuously_lift_exact_ring_with_work(
    frame: GeographicFrame,
    exact_ports: &[ExactChartPoint],
    work: &mut impl Work,
) -> Result<Vec<ExactChartPoint>> {
    let Some((first, remaining)) = exact_ports.split_first() else {
        return Err(GeometryErrorKind::Invalid("Q exact ring has no ports".into()).into());
    };
    if exact_ports.len() < 4 {
        return Err(GeometryErrorKind::Invalid(
            "Q exact ring has fewer than three vertices".into(),
        )
        .into());
    }
    let Some((last, interior)) = remaining.split_last() else {
        return Err(GeometryErrorKind::Invalid("Q exact ring has no closing port".into()).into());
    };
    if !first.y.try_cmp_with_work(&last.y, work)?.is_eq() {
        return Err(GeometryErrorKind::Invalid(
            "Q exact ring endpoints have different latitudes".into(),
        )
        .into());
    }

    work.charge_into_error(
        u64::try_from(exact_ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut lifted = Vec::with_capacity(exact_ports.len());
    let mut reference = first.x.try_clone_with_work(work)?;
    lifted.push(first.try_clone_with_work(work)?);
    for point in interior {
        work.charge_into_error(1)?;
        let longitude = nearest_exact_longitude_alias_with_work(frame, &point.x, &reference, work)?;
        reference = longitude.try_clone_with_work(work)?;
        lifted.push(ExactChartPoint::from_rationals(
            longitude,
            point.y.try_clone_with_work(work)?,
        ));
    }
    lifted.push(first.try_clone_with_work(work)?);

    Ok(lifted)
}

#[cfg(test)]
pub(in crate::geometry::topology) fn exact_ring_winding(
    points: &[ExactChartPoint],
) -> Result<RingWinding> {
    let mut work = Unmetered;
    exact_ring_winding_with_work(points, &mut work)
}

pub(in crate::geometry::topology) fn exact_ring_winding_with_work(
    points: &[ExactChartPoint],
    work: &mut impl Work,
) -> Result<RingWinding> {
    let origin = points
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q lifted ring has no points".into()))?;
    let mut twice_area = ExactRational::zero();
    for pair in points.windows(2) {
        work.charge_into_error(1)?;
        let [start, end] = pair else {
            unreachable!("windows(2) contains two exact ring points");
        };
        let start_x = start.x.try_subtract_with_work(&origin.x, work)?;
        let start_y = start.y.try_subtract_with_work(&origin.y, work)?;
        let end_x = end.x.try_subtract_with_work(&origin.x, work)?;
        let end_y = end.y.try_subtract_with_work(&origin.y, work)?;
        twice_area = twice_area.try_add_with_work(
            &start_x
                .try_product_with_work(&end_y, work)?
                .try_subtract_with_work(&end_x.try_product_with_work(&start_y, work)?, work)?,
            work,
        )?;
    }
    Ok(match twice_area.sign() {
        ExactSign::Negative => RingWinding::Clockwise,
        ExactSign::Zero => RingWinding::Degenerate,
        ExactSign::Positive => RingWinding::CounterClockwise,
    })
}

#[cfg(test)]
pub(in crate::geometry::topology) fn rounded_exact_ring(
    points: &[ExactChartPoint],
) -> Result<Ring> {
    let mut work = Unmetered;
    rounded_exact_ring_with_work(points, &mut work)
}

pub(in crate::geometry::topology) fn rounded_exact_ring_with_work(
    points: &[ExactChartPoint],
    work: &mut impl Work,
) -> Result<Ring> {
    work.charge_into_error(u64::try_from(points.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let points = points
        .iter()
        .map(|point| {
            work.charge_into_error(1)?;
            Ok(Point::new_raw_xy(
                point.x.round_with_work(work)?,
                point.y.round_with_work(work)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ring::try_from_closed(CoordSeq::try_from_points(&points)?)
}

pub(in crate::geometry::topology) fn exact_canonical_longitude_with_work(
    frame: GeographicFrame,
    longitude: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactRational> {
    let axis = frame.longitude();
    let half = ExactRational::from_f64(axis.half_period());
    let negative_half = ExactRational::zero().try_subtract_with_work(&half, work)?;
    let period = ExactRational::from_f64(axis.period());
    if longitude.try_cmp_with_work(&half, work)?.is_ge() {
        longitude.try_subtract_with_work(&period, work)
    } else if longitude.try_cmp_with_work(&negative_half, work)?.is_lt() {
        longitude.try_add_with_work(&period, work)
    } else {
        longitude.try_clone_with_work(work)
    }
}

pub(in crate::geometry::topology) fn exact_distance_with_work(
    left: &ExactRational,
    right: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactRational> {
    let difference = left.try_subtract_with_work(right, work)?;
    if difference
        .try_cmp_with_work(&ExactRational::zero(), work)?
        .is_lt()
    {
        ExactRational::zero().try_subtract_with_work(&difference, work)
    } else {
        Ok(difference)
    }
}

#[cfg(test)]
pub(in crate::geometry::topology) fn nearest_exact_longitude_alias(
    frame: GeographicFrame,
    raw: &ExactRational,
    reference: &ExactRational,
) -> Result<ExactRational> {
    let mut work = Unmetered;
    nearest_exact_longitude_alias_with_work(frame, raw, reference, &mut work)
}

pub(in crate::geometry::topology) fn nearest_exact_longitude_alias_with_work(
    frame: GeographicFrame,
    raw: &ExactRational,
    reference: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactRational> {
    let period = ExactRational::from_f64(frame.longitude().period());
    let canonical = exact_canonical_longitude_with_work(frame, raw, work)?;
    let candidates = [
        canonical.try_subtract_with_work(&period, work)?,
        canonical.try_clone_with_work(work)?,
        canonical.try_add_with_work(&period, work)?,
    ];
    let mut nearest = None;
    for candidate in candidates {
        work.charge_into_error(1)?;
        let distance = exact_distance_with_work(&candidate, reference, work)?;
        let replace = if let Some((previous, previous_distance)) = &nearest {
            match distance.try_cmp_with_work(previous_distance, work)? {
                Ordering::Less => true,
                Ordering::Equal => {
                    let candidate_offset = exact_distance_with_work(&candidate, raw, work)?;
                    let previous_offset = exact_distance_with_work(previous, raw, work)?;
                    match candidate_offset.try_cmp_with_work(&previous_offset, work)? {
                        Ordering::Less => true,
                        Ordering::Equal => candidate.try_cmp_with_work(previous, work)?.is_lt(),
                        Ordering::Greater => false,
                    }
                },
                Ordering::Greater => false,
            }
        } else {
            true
        };
        if replace {
            nearest = Some((candidate, distance));
        }
    }
    nearest.map(|(longitude, _)| longitude).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q exact ring has no chart longitude".into()).into()
    })
}

#[cfg(test)]
pub(in crate::geometry::topology) fn round_canonical_longitude(
    frame: GeographicFrame,
    longitude: &ExactRational,
) -> Result<f64> {
    let mut work = Unmetered;
    round_canonical_longitude_with_work(frame, longitude, &mut work)
}

pub(in crate::geometry::topology) fn round_canonical_longitude_with_work(
    frame: GeographicFrame,
    longitude: &ExactRational,
    work: &mut impl Work,
) -> Result<f64> {
    let axis = frame.longitude();
    let half = ExactRational::from_f64(axis.half_period());
    let period = ExactRational::from_f64(axis.period());
    let negative_half = ExactRational::zero().try_subtract_with_work(&half, work)?;
    let canonical = if longitude.try_cmp_with_work(&half, work)?.is_ge() {
        longitude.try_subtract_with_work(&period, work)?
    } else if longitude.try_cmp_with_work(&negative_half, work)?.is_lt() {
        longitude.try_add_with_work(&period, work)?
    } else {
        longitude.try_clone_with_work(work)?
    };
    // A rounded chart alias such as `122.427194 + 360` loses source low bits
    // before floating remainder can canonicalize it.
    axis.canonicalize(canonical.round_with_work(work)?)
        .map(|longitude| longitude.get())
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()).into())
}

pub(in crate::geometry::topology) enum QuotientRingRoleStatus {
    Unclassifiable,
    Classified(bool),
    Unbounded,
}

pub(in crate::geometry::topology) fn quotient_ring_role_status(
    frame: GeographicFrame,
    ring: &Ring,
) -> Result<QuotientRingRoleStatus> {
    let axis = frame.longitude();
    if ring
        .coords()
        .xs()
        .array_windows::<2>()
        .all(|&[from, to]| axis.ordinary_chart_edge(from, to))
    {
        return Ok(QuotientRingRoleStatus::Unclassifiable);
    }
    let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    if summary.exact_axis_band {
        let south = canonical_bits(summary.south.get());
        let mut previous: Option<Point> = None;
        let mut south_turn = 0_i64;
        for point in ring.coords().points() {
            if let Some(previous) = previous
                && canonical_bits(previous.y) == south
                && canonical_bits(point.y) == south
            {
                let connection = axis
                    .chart_connection(previous.x, point.x)
                    .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
                south_turn = south_turn
                    .checked_add(i64::from(connection.source_lift))
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            previous = Some(point);
        }
        if south_turn != 0 {
            return Ok(QuotientRingRoleStatus::Classified(south_turn < 0));
        }
    }
    if summary.chart_turn == 0 {
        return Ok(QuotientRingRoleStatus::Unclassifiable);
    }
    match summary.bounded_pole() {
        Some(ChartPole::North) => Ok(QuotientRingRoleStatus::Classified(summary.chart_turn < 0)),
        Some(ChartPole::South) => Ok(QuotientRingRoleStatus::Classified(summary.chart_turn > 0)),
        None => Ok(QuotientRingRoleStatus::Unbounded),
    }
}

pub(in crate::geometry::topology) fn exact_ring_bounded_pole(
    points: &[ExactChartPoint],
    work: &mut impl Work,
) -> Result<Option<ChartPole>> {
    // Name the cap from exact Q ordinates rather than the rounded role ring.
    let zero = ExactRational::zero();
    let mut north = false;
    let mut south = false;
    work.charge_into_error(u64::try_from(points.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    for point in points {
        work.charge_into_error(1)?;
        match point.y.try_cmp_with_work(&zero, work)? {
            Ordering::Less => south = true,
            Ordering::Equal => {},
            Ordering::Greater => north = true,
        }
    }
    Ok(match (north, south) {
        (true, false) => Some(ChartPole::North),
        (false, true) => Some(ChartPole::South),
        (false, false) | (true, true) => None,
    })
}
