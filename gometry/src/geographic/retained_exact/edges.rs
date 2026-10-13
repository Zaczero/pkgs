//! Retained geographic edges arithmetic.

#[cfg(test)]
use super::sites::retained_exact_native_site;
use super::sites::{
    ExactNativeLongitude, RetainedComputedSample, exact_integer_i64, split_rational_native_site,
};
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::{ExactDyadic, ExactRational};

pub(super) fn retained_edge_displacement(
    edge: crate::geographic::RetainedChartDisplacement,
    axis: crate::geographic::PeriodicAxis,
) -> crate::error::Result<ExactDyadic> {
    let [start, end] = *edge.endpoints();
    ExactDyadic::from_f64(end.get())
        .try_subtract(ExactDyadic::from_f64(start.get()))?
        .try_add(
            ExactDyadic::from_f64(axis.period())
                .try_product(&ExactDyadic::from_i64(edge.periods()))?,
        )
}

fn retained_edge_delta(
    edge: crate::geographic::RetainedChartDisplacement,
    axis: crate::geographic::PeriodicAxis,
) -> crate::error::Result<ExactRational> {
    Ok(ExactRational::from_dyadic(retained_edge_displacement(
        edge, axis,
    )?))
}

fn exact_lifted_edge_delta(endpoints: [&ExactRational; 2]) -> crate::error::Result<ExactRational> {
    endpoints[1].try_subtract(endpoints[0])
}

pub(crate) fn retained_edge_from_lifted_binary64_pair(
    axis: crate::geographic::PeriodicAxis,
    endpoints: [f64; 2],
) -> crate::error::Result<crate::geographic::RetainedChartDisplacement> {
    let [start, end] = endpoints;
    if !(start.is_finite() && end.is_finite()) {
        return Err(crate::geometry::GeometryErrorKind::NonFiniteCoordinate.into());
    }
    if axis.ordinary_chart_edge(start, end) {
        let start = axis.canonicalize(start).map_err(|error| {
            crate::geometry::GeometryErrorKind::Invalid(error.to_string().into())
        })?;
        let end = axis.canonicalize(end).map_err(|error| {
            crate::geometry::GeometryErrorKind::Invalid(error.to_string().into())
        })?;
        return Ok(
            crate::geographic::RetainedChartDisplacement::from_native_endpoints([start, end], 0),
        );
    }

    let split = |longitude: f64| -> crate::error::Result<_> {
        let exact = ExactRational::from_f64(longitude);
        let (native, sheet) =
            split_rational_native_site(&exact, axis.period(), axis.half_period())?;
        let native = ExactNativeLongitude::from_rational(axis, native.into_owned())?;
        let (native, rounding_carry) = ExactNativeLongitude::round_native(axis, &native)?;
        let sheet = sheet.try_add(ExactDyadic::from_i64(i64::from(rounding_carry)))?;
        Ok((native, sheet))
    };
    let (start_native, start_sheet) = split(start)?;
    let (end_native, end_sheet) = split(end)?;
    let relative_sheet = end_sheet.try_subtract(start_sheet)?;
    let relative_periods = exact_integer_i64(&relative_sheet)?;
    Ok(
        crate::geographic::RetainedChartDisplacement::from_native_endpoints(
            [start_native, end_native],
            relative_periods,
        ),
    )
}

/// Rebuild a same-frame retained relation from the endpoint pair and the
/// producer's discrete chart class. `Minor` magnitude is deliberately ignored;
/// a half-turn witness selects one of the two valid directed branches.
pub(crate) fn retained_edge_from_chart_witness(
    axis: crate::geographic::PeriodicAxis,
    endpoints: [f64; 2],
    witness: crate::geographic::ChartStep,
) -> crate::error::Result<crate::geographic::RetainedChartDisplacement> {
    use crate::geographic::{ChartStep, EdgeDirection, RetainedChartDisplacement, TurnDirection};

    let connection = axis
        .chart_connection(endpoints[0], endpoints[1])
        .map_err(|error| crate::geometry::GeometryErrorKind::Invalid(error.to_string().into()))?;
    let natives = [connection.start, connection.end];
    let relation = match witness {
        ChartStep::Circle(direction) => {
            if connection.step != ChartStep::Circle(direction) {
                return Err(crate::geometry::GeometryErrorKind::Invalid(
                    "stored chart circle disagrees with its represented endpoint pair".into(),
                )
                .into());
            }
            let sign = if direction == TurnDirection::Negative {
                -1.0
            } else {
                1.0
            };
            let target = ExactRational::from_f64(sign * axis.period());
            let periods = retained_periods_for_exact_delta(axis, natives, &target)?;
            RetainedChartDisplacement::from_native_endpoints(natives, periods)
        },
        ChartStep::Shortest(EdgeDirection::HalfTurn(direction)) => {
            let sign = if direction == TurnDirection::Negative {
                -1.0
            } else {
                1.0
            };
            let target = ExactRational::from_f64(sign * axis.half_period());
            let periods = retained_periods_for_exact_delta(axis, natives, &target)?;
            let edge = RetainedChartDisplacement::from_native_endpoints(natives, periods);
            if retained_native_interval_class(edge, axis, 0.0, 1.0)?
                != crate::geographic::RetainedLongitudeClass::HalfTurn(direction)
            {
                return Err(crate::geometry::GeometryErrorKind::Invalid(
                    "stored half-turn direction disagrees with its represented endpoint pair"
                        .into(),
                )
                .into());
            }
            edge
        },
        ChartStep::Shortest(EdgeDirection::Minor(0.0)) => {
            let edge = RetainedChartDisplacement::from_native_endpoints(natives, 0);
            if retained_native_interval_class(edge, axis, 0.0, 1.0)?
                != crate::geographic::RetainedLongitudeClass::Zero
            {
                return Err(crate::geometry::GeometryErrorKind::Invalid(
                    "stored zero chart step disagrees with its represented endpoint pair".into(),
                )
                .into());
            }
            edge
        },
        ChartStep::Shortest(EdgeDirection::Minor(delta)) => {
            let direction = if delta.is_sign_negative() {
                TurnDirection::Negative
            } else {
                TurnDirection::Positive
            };
            let edge = connection.retained();
            if retained_native_interval_class(edge, axis, 0.0, 1.0)?
                != crate::geographic::RetainedLongitudeClass::Minor(direction)
            {
                return Err(crate::geometry::GeometryErrorKind::Invalid(
                    "stored minor direction disagrees with its represented endpoint pair".into(),
                )
                .into());
            }
            edge
        },
    };
    Ok(relation)
}

fn retained_periods_for_exact_delta(
    axis: crate::geographic::PeriodicAxis,
    endpoints: [crate::geographic::CanonicalLongitude; 2],
    target_delta: &ExactRational,
) -> crate::error::Result<i64> {
    let native_delta = ExactRational::from_f64(endpoints[1].get())
        .try_subtract(&ExactRational::from_f64(endpoints[0].get()))?;
    let relative_lift = target_delta
        .try_subtract(&native_delta)?
        .try_quotient(&ExactRational::from_f64(axis.period()))?;
    let (numerator, denominator) = relative_lift.parts();
    if !denominator.is_one() {
        return Err(crate::geometry::GeometryErrorKind::Invalid(
            "stored chart class does not select an integral endpoint lift".into(),
        )
        .into());
    }
    exact_integer_i64(numerator)
}

fn exact_parameter_span(from: f64, to: f64) -> crate::error::Result<ExactRational> {
    if !(from.is_finite()
        && to.is_finite()
        && (0.0..=1.0).contains(&from)
        && (0.0..=1.0).contains(&to)
        && from <= to)
    {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "computed edge",
        ));
    }
    ExactRational::from_f64(to).try_subtract(&ExactRational::from_f64(from))
}

fn exact_delta_class(
    delta: &ExactRational,
    axis: crate::geographic::PeriodicAxis,
) -> crate::error::Result<crate::geographic::RetainedLongitudeClass> {
    use std::cmp::Ordering;

    use crate::geographic::{RetainedLongitudeClass as Class, TurnDirection};

    if delta.is_zero() {
        return Ok(Class::Zero);
    }
    let direction = match delta.sign() {
        ExactSign::Negative => TurnDirection::Negative,
        ExactSign::Zero => return Ok(Class::Zero),
        ExactSign::Positive => TurnDirection::Positive,
    };
    let (numerator, denominator) = delta.parts();
    let magnitude = if direction == TurnDirection::Negative {
        ExactRational::new(numerator.clone().neg(), denominator.clone())?
    } else {
        delta.clone()
    };
    match magnitude.try_cmp(&ExactRational::from_f64(axis.half_period()))? {
        Ordering::Less => Ok(Class::Minor(direction)),
        Ordering::Equal => Ok(Class::HalfTurn(direction)),
        Ordering::Greater => {
            if magnitude
                .try_cmp(&ExactRational::from_f64(axis.period()))?
                .is_eq()
            {
                Ok(Class::Circle(direction))
            } else {
                Ok(Class::Major(direction))
            }
        },
    }
}

pub(crate) fn retained_native_interval_class(
    edge: crate::geographic::RetainedChartDisplacement,
    axis: crate::geographic::PeriodicAxis,
    from: f64,
    to: f64,
) -> crate::error::Result<crate::geographic::RetainedLongitudeClass> {
    let span = exact_parameter_span(from, to)?;
    let delta = retained_edge_delta(edge, axis)?.try_product(&span)?;
    exact_delta_class(&delta, axis)
}

pub(crate) fn retained_exact_interval_class(
    endpoints: [&ExactRational; 2],
    axis: crate::geographic::PeriodicAxis,
    from: f64,
    to: f64,
) -> crate::error::Result<crate::geographic::RetainedLongitudeClass> {
    let span = exact_parameter_span(from, to)?;
    let delta = exact_lifted_edge_delta(endpoints)?.try_product(&span)?;
    exact_delta_class(&delta, axis)
}

fn strict_subhalf_piece_count(
    delta: &ExactRational,
    axis: crate::geographic::PeriodicAxis,
) -> crate::error::Result<usize> {
    use std::cmp::Ordering;

    let (numerator, denominator) = delta.parts();
    let magnitude = if delta.sign() == ExactSign::Negative {
        ExactRational::new(numerator.clone().neg(), denominator.clone())?
    } else {
        delta.clone()
    };
    let ratio = magnitude.try_quotient(&ExactRational::from_f64(axis.half_period()))?;
    let (ratio_numerator, ratio_denominator) = ratio.parts();
    let nearest = ratio_numerator.nearest_integer_quotient_ties_to_zero(ratio_denominator)?;
    let rounded = exact_nonnegative_usize(&nearest)?;
    let nearest_ratio = ExactRational::from_dyadic(nearest);
    let strict = if ratio.try_cmp(&nearest_ratio)? == Ordering::Greater {
        rounded
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?
    } else {
        rounded
    };
    Ok(strict.max(2))
}

pub(crate) fn retained_native_split_piece_count(
    edge: crate::geographic::RetainedChartDisplacement,
    axis: crate::geographic::PeriodicAxis,
) -> crate::error::Result<usize> {
    strict_subhalf_piece_count(&retained_edge_delta(edge, axis)?, axis)
}

pub(crate) fn retained_exact_split_piece_count(
    endpoints: [&ExactRational; 2],
    axis: crate::geographic::PeriodicAxis,
) -> crate::error::Result<usize> {
    strict_subhalf_piece_count(&exact_lifted_edge_delta(endpoints)?, axis)
}

fn written_exact_longitude(
    axis: crate::geographic::PeriodicAxis,
    position: &ExactRational,
) -> crate::error::Result<RetainedComputedSample> {
    let (native, sheet) = split_rational_native_site(position, axis.period(), axis.half_period())?;
    let native = ExactNativeLongitude::from_rational(axis, native.into_owned())?;
    let (written, adjustment) = ExactNativeLongitude::round_native(axis, &native)?;
    let sheet = sheet.try_add(ExactDyadic::from_i64(i64::from(adjustment)))?;
    let chart_longitude = ExactRational::from_f64(written.get()).try_add(
        &ExactRational::from_f64(axis.period()).try_product(&ExactRational::from_dyadic(sheet))?,
    )?;
    Ok(RetainedComputedSample {
        longitude: written,
        chart_longitude,
    })
}

/// Round one exact constructed chart longitude through the shared native
/// quotient and seam-carry owner.
pub(crate) fn retained_exact_site_sample(
    axis: crate::geographic::PeriodicAxis,
    position: &ExactRational,
) -> crate::error::Result<RetainedComputedSample> {
    written_exact_longitude(axis, position)
}

#[expect(
    clippy::float_cmp,
    reason = "zero and one are exact publisher parameters selecting authentic endpoint records"
)]
pub(crate) fn retained_native_publisher_sample(
    axis: crate::geographic::PeriodicAxis,
    edge: crate::geographic::RetainedChartDisplacement,
    parameter: f64,
) -> crate::error::Result<RetainedComputedSample> {
    if !(parameter.is_finite() && (0.0..=1.0).contains(&parameter)) {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "computed edge",
        ));
    }
    let [start, end] = *edge.endpoints();
    if parameter == 0.0 {
        return Ok(RetainedComputedSample {
            longitude: start,
            chart_longitude: ExactRational::from_f64(start.get()),
        });
    }
    if parameter == 1.0 {
        let exact_end = ExactRational::from_f64(end.get()).try_add(
            &ExactRational::from_f64(axis.period()).try_product(&ExactRational::from_dyadic(
                ExactDyadic::from_i64(edge.periods()),
            ))?,
        )?;
        return Ok(RetainedComputedSample {
            longitude: end,
            chart_longitude: exact_end,
        });
    }
    let displacement = retained_edge_delta(edge, axis)?;
    let position = ExactRational::from_f64(start.get())
        .try_add(&displacement.try_product(&ExactRational::from_f64(parameter))?)?;
    retained_exact_site_sample(axis, &position)
}

#[expect(
    clippy::float_cmp,
    reason = "zero and one are exact publisher parameters selecting authentic endpoint records"
)]
pub(crate) fn retained_exact_publisher_sample(
    axis: crate::geographic::PeriodicAxis,
    endpoints: [&ExactRational; 2],
    parameter: f64,
) -> crate::error::Result<RetainedComputedSample> {
    if !(parameter.is_finite() && (0.0..=1.0).contains(&parameter)) {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "computed edge",
        ));
    }
    let position = if parameter == 0.0 {
        endpoints[0].clone()
    } else if parameter == 1.0 {
        endpoints[1].clone()
    } else {
        let delta = exact_lifted_edge_delta(endpoints)?;
        endpoints[0].try_add(&delta.try_product(&ExactRational::from_f64(parameter))?)?
    };
    retained_exact_site_sample(axis, &position)
}

/// Recover a local retained relation from two emitted samples.
///
/// Subtract the exact chart positions before narrowing the relative period
/// count; their absolute sheets are never required to fit an integer type.
pub(crate) fn retained_computed_samples_connection(
    axis: crate::geographic::PeriodicAxis,
    samples: [&RetainedComputedSample; 2],
) -> crate::error::Result<crate::geographic::RetainedChartDisplacement> {
    let [start, end] = samples;
    let chart_delta = end.chart_longitude.try_subtract(&start.chart_longitude)?;
    let native_delta = ExactRational::from_f64(end.longitude.get())
        .try_subtract(&ExactRational::from_f64(start.longitude.get()))?;
    let sheet_delta = chart_delta.try_subtract(&native_delta)?;
    let relative_periods = sheet_delta.try_quotient(&ExactRational::from_f64(axis.period()))?;
    let (numerator, denominator) = relative_periods.parts();
    if !denominator.is_one() {
        return Err(crate::geometry::GeometryErrorKind::Invalid(
            "published retained endpoints do not differ by an integral period lift".into(),
        )
        .into());
    }
    let periods = exact_integer_i64(numerator)?;
    Ok(
        crate::geographic::RetainedChartDisplacement::from_native_endpoints(
            [start.longitude, end.longitude],
            periods,
        ),
    )
}

/// Check that the written XY step preserves the exact source interval's
/// direction and branch class.
///
/// Generated half-turns require refinement. A nonzero nonconstant interval
/// may still be readable when one ordinate rounds to equality, provided the
/// other coordinate carries the interval.
#[expect(
    clippy::float_cmp,
    reason = "zero, direction, pole identity, and branch classes are discrete"
)]
pub(crate) fn retained_source_interval_reads_as(
    edge: crate::geographic::RetainedChartDisplacement,
    frame: crate::geographic::GeographicFrame,
    source_latitudes: [f64; 2],
    from: f64,
    to: f64,
    readback: crate::geographic::ChartStep,
    start: crate::geometry::Point,
    end: crate::geometry::Point,
) -> crate::error::Result<bool> {
    use std::cmp::Ordering;

    use crate::geographic::{ChartStep, EdgeDirection, Latitude, SeamIdentity, TurnDirection};

    let axis = frame.longitude();
    if !(from.is_finite()
        && to.is_finite()
        && (0.0..=1.0).contains(&from)
        && (0.0..=1.0).contains(&to)
        && from <= to)
    {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "computed edge",
        ));
    }
    let parameter_delta = ExactDyadic::from_f64(to).try_subtract(ExactDyadic::from_f64(from))?;
    let source_longitude_delta = retained_edge_displacement(edge, axis)?;
    let delta = source_longitude_delta.try_product(&parameter_delta)?;
    let source_latitude_delta = ExactDyadic::from_f64(source_latitudes[1])
        .try_subtract(ExactDyadic::from_f64(source_latitudes[0]))?;
    let latitude_delta = source_latitude_delta.try_product(&parameter_delta)?;
    let start_latitude = Latitude::from_admitted(frame, start.y).get();
    let end_latitude = Latitude::from_admitted(frame, end.y).get();

    let source_start_latitude = Latitude::from_admitted(frame, source_latitudes[0]);
    let source_end_latitude = Latitude::from_admitted(frame, source_latitudes[1]);
    let same_source_pole = source_start_latitude == source_end_latitude
        && source_start_latitude.identity(frame) == SeamIdentity::Pole;
    let root_is_constant =
        source_latitude_delta.is_zero() && (source_longitude_delta.is_zero() || same_source_pole);
    let output_xy_collapsed = start.x == end.x && start_latitude == end_latitude;
    // Quantization may hide movement in either axis alone; only joint XY
    // collapse hides a nonconstant source interval completely.
    if !parameter_delta.is_zero() && output_xy_collapsed && !root_is_constant {
        return Ok(false);
    }
    if root_is_constant
        && output_xy_collapsed
        && !matches!(readback, ChartStep::Shortest(EdgeDirection::HalfTurn(_)))
    {
        return Ok(true);
    }

    let longitude_delta_is_zero = delta.is_zero();
    let negative = delta.negative;
    let direction_matches = |direction| {
        matches!(
            (direction, negative),
            (TurnDirection::Negative, true) | (TurnDirection::Positive, false)
        )
    };
    let magnitude = delta.abs();
    let half_period = ExactDyadic::from_f64(axis.half_period());
    let period = ExactDyadic::from_f64(axis.period());
    let longitude_matches = match readback {
        ChartStep::Shortest(EdgeDirection::Minor(value)) if longitude_delta_is_zero => value == 0.0,
        ChartStep::Shortest(EdgeDirection::Minor(value)) => {
            magnitude.cmp(&half_period) == Ordering::Less
                && value != 0.0
                && value.is_sign_negative() == negative
        },
        ChartStep::Shortest(EdgeDirection::HalfTurn(_)) => false,
        ChartStep::Circle(direction) => {
            magnitude.cmp(&period) == Ordering::Equal && direction_matches(direction)
        },
    };
    let latitude_matches = if latitude_delta.is_zero() {
        start_latitude == end_latitude
    } else if latitude_delta.negative {
        end_latitude <= start_latitude
    } else {
        end_latitude >= start_latitude
    };
    Ok(longitude_matches && latitude_matches)
}

fn exact_nonnegative_usize(value: &ExactDyadic) -> crate::error::Result<usize> {
    if value.is_zero() {
        return Ok(0);
    }
    if value.negative || value.exponent < 0 {
        return Err(crate::error::ResourceError::SizeOverflow.into());
    }
    let magnitude = value
        .small_magnitude()
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let shift =
        u32::try_from(value.exponent).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    if shift >= usize::BITS || magnitude > (usize::MAX as u128 >> shift) {
        return Err(crate::error::ResourceError::SizeOverflow.into());
    }
    Ok((magnitude as usize) << shift)
}

/// Initial equal-parameter intervals with exact longitude displacement no
/// greater than a half-period. The written chain is checked separately and
/// may need further subdivision after source-sheet rounding.
pub(crate) fn retained_source_interval_initial_piece_count(
    edge: crate::geographic::RetainedChartDisplacement,
    axis: crate::geographic::PeriodicAxis,
    from: f64,
    to: f64,
) -> crate::error::Result<usize> {
    use std::cmp::Ordering;

    if !(from.is_finite()
        && to.is_finite()
        && (0.0..=1.0).contains(&from)
        && (0.0..=1.0).contains(&to)
        && from <= to)
    {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "line_substring",
        ));
    }
    let parameter_delta = ExactDyadic::from_f64(to).try_subtract(ExactDyadic::from_f64(from))?;
    let displacement = retained_edge_displacement(edge, axis)?;
    let exact_delta = displacement.try_product(&parameter_delta)?.abs();
    let half_period = ExactDyadic::from_f64(axis.half_period());
    let nearest = exact_delta.nearest_integer_quotient_ties_to_zero(&half_period)?;
    let nearest_span = half_period.try_product(&nearest)?;
    let ceiling = if nearest_span.cmp(&exact_delta) == Ordering::Less {
        nearest.try_add(ExactDyadic::one())?
    } else {
        nearest
    };
    Ok(exact_nonnegative_usize(&ceiling)?.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::{GeographicFrame, RetainedChartDisplacement, RetainedLongitudeSite};

    #[test]
    fn exact_nonnegative_usize_admits_zero_and_rejects_fractional_negative_and_overflow() {
        assert_eq!(exact_nonnegative_usize(&ExactDyadic::zero()).unwrap(), 0);
        assert_eq!(exact_nonnegative_usize(&ExactDyadic::one()).unwrap(), 1);
        assert_eq!(
            exact_nonnegative_usize(&ExactDyadic::from_i64(2)).unwrap(),
            2
        );
        assert_eq!(
            exact_nonnegative_usize(&ExactDyadic::from_parts(false, vec![u64::MAX], 0,)).unwrap(),
            usize::MAX
        );

        for invalid in [
            ExactDyadic::from_f64(0.5),
            ExactDyadic::from_i64(-1),
            ExactDyadic::from_parts(false, vec![0, 1], 0),
            ExactDyadic::from_parts(false, vec![1], i64::from(usize::BITS)),
            ExactDyadic::from_parts(false, vec![3], i64::from(usize::BITS - 1)),
        ] {
            assert!(matches!(
                exact_nonnegative_usize(&invalid).unwrap_err().kind(),
                crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
            ));
        }
    }
    #[test]
    fn exact_retained_sites_keep_rational_endpoints_and_assigned_lift_correspondence() {
        let axis = GeographicFrame::crs84().longitude();
        let rational = |numerator| {
            ExactRational::new(ExactDyadic::from_i64(numerator), ExactDyadic::from_i64(3)).unwrap()
        };
        let one_third = rational(1);
        let constructed_one_third =
            ExactNativeLongitude::from_rational(axis, one_third.clone()).unwrap();
        let projected_one_third =
            retained_exact_native_site(axis, constructed_one_third.rational()).unwrap();
        assert_eq!(
            projected_one_third.native().rational(),
            &one_third,
            "a constructed non-dyadic endpoint must remain exact in the native window"
        );
        assert_eq!(projected_one_third.periods(), 0);

        let start_native = ExactNativeLongitude::from_rational(axis, rational(538)).unwrap();
        let end_native = ExactNativeLongitude::from_rational(axis, rational(-538)).unwrap();
        let assigned_start = RetainedLongitudeSite::new(start_native.clone(), -3);
        let assigned_end = RetainedLongitudeSite::new(end_native.clone(), -2);
        let assigned_edge = RetainedChartDisplacement::between_sites([
            assigned_start.clone(),
            assigned_end.clone(),
        ])
        .unwrap();
        assert_eq!(assigned_edge.periods(), 1);
        assert_eq!(assigned_edge.endpoints()[0], start_native);
        assert_eq!(assigned_edge.endpoints()[1], end_native);

        let period = ExactRational::from_f64(axis.period());
        let delta = end_native
            .rational()
            .try_subtract(start_native.rational())
            .unwrap()
            .try_add(
                &period
                    .try_product(&ExactRational::from_dyadic(ExactDyadic::from_i64(
                        assigned_edge.periods(),
                    )))
                    .unwrap(),
            )
            .unwrap();
        let exact_parameter = ExactRational::from_f64(0.5);
        let lifted_midpoint = start_native
            .rational()
            .try_add(&delta.try_product(&exact_parameter).unwrap())
            .unwrap();
        assert_eq!(lifted_midpoint, ExactRational::from_f64(180.0));
        let midpoint = retained_exact_native_site(axis, &lifted_midpoint).unwrap();
        assert_eq!(
            midpoint.native().rational(),
            &ExactRational::from_f64(-180.0)
        );
        assert_eq!(midpoint.periods(), 1);
        let published = ExactNativeLongitude::round_site(axis, &midpoint).unwrap();
        assert_eq!(published.native().get(), -180.0);
        assert_eq!(published.periods(), 1);
        assert_eq!(
            axis.canonicalize(published.native().get()).unwrap(),
            *published.native()
        );

        let sub_ulp_gap =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 47)).unwrap();
        let just_below_positive_half = ExactRational::from_f64(axis.half_period())
            .try_subtract(&sub_ulp_gap)
            .unwrap();
        let exact_positive_site =
            retained_exact_native_site(axis, &just_below_positive_half).unwrap();
        assert_eq!(
            exact_positive_site.native().rational(),
            &just_below_positive_half
        );
        assert_eq!(exact_positive_site.periods(), 0);
        let rounded_positive_site =
            ExactNativeLongitude::round_site(axis, &exact_positive_site).unwrap();
        assert_eq!(rounded_positive_site.native().get(), -180.0);
        assert_eq!(rounded_positive_site.periods(), 1);
        assert!(
            (-180.0..180.0).contains(&rounded_positive_site.native().get()),
            "rounded exact site must retain its canonical native spelling"
        );

        let just_below_negative_half = ExactRational::from_f64(-axis.half_period())
            .try_subtract(&sub_ulp_gap)
            .unwrap();
        let exact_negative_site =
            retained_exact_native_site(axis, &just_below_negative_half).unwrap();
        assert_eq!(
            exact_negative_site.native().rational(),
            &just_below_positive_half
        );
        assert_eq!(exact_negative_site.periods(), -1);
        let rounded_negative_site =
            ExactNativeLongitude::round_site(axis, &exact_negative_site).unwrap();
        assert_eq!(rounded_negative_site.native().get(), -180.0);
        assert_eq!(rounded_negative_site.periods(), 0);
        assert_eq!(
            axis.canonicalize(rounded_negative_site.native().get())
                .unwrap(),
            *rounded_negative_site.native()
        );

        let relative_start = retained_exact_native_site(axis, start_native.rational()).unwrap();
        let lifted_end = end_native.rational().try_add(&period).unwrap();
        let relative_end = retained_exact_native_site(axis, &lifted_end).unwrap();
        let actual_start = RetainedLongitudeSite::new(
            relative_start.native().clone(),
            assigned_start
                .periods()
                .checked_add(relative_start.periods())
                .unwrap(),
        );
        let actual_end = RetainedLongitudeSite::new(
            relative_end.native().clone(),
            assigned_start
                .periods()
                .checked_add(relative_end.periods())
                .unwrap(),
        );
        assert_eq!(actual_start, assigned_start);
        assert_eq!(actual_end, assigned_end);

        let actual_midpoint = RetainedLongitudeSite::new(
            midpoint.native().clone(),
            assigned_start
                .periods()
                .checked_add(midpoint.periods())
                .unwrap(),
        );
        assert_eq!(
            actual_midpoint.native().rational(),
            &ExactRational::from_f64(-180.0)
        );
        assert_eq!(actual_midpoint.periods(), -2);
    }
}
