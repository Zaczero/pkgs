//! Retained geographic sites arithmetic.

use super::edges::retained_edge_displacement;
use crate::geographic::retained_native_private::Sealed;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::numerics::product_sum;
use crate::geometry::{ExactDyadic, ExactRational};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExactNativeLongitude(ExactRational);

/// The exact root sample and the exact chart position of its rounded spelling.
///
/// Keeping both prevents a written endpoint from replacing its producer's
/// exact root relation.
pub(crate) struct RetainedComputedSample {
    pub(crate) longitude: crate::geographic::CanonicalLongitude,
    pub(crate) chart_longitude: ExactRational,
}

impl ExactNativeLongitude {
    pub(crate) fn from_rational(
        axis: crate::geographic::PeriodicAxis,
        value: ExactRational,
    ) -> crate::error::Result<Self> {
        let mut work = crate::work::Unmetered;
        Self::from_rational_with_work(axis, value, &mut work)
    }

    pub(crate) fn from_rational_with_work(
        axis: crate::geographic::PeriodicAxis,
        value: ExactRational,
        work: &mut impl crate::work::Work,
    ) -> crate::error::Result<Self> {
        use std::cmp::Ordering;

        let half_period = ExactRational::from_f64(axis.half_period());
        let negative_half_period = ExactRational::from_f64(-axis.half_period());
        if value.try_cmp_with_work(&negative_half_period, work)? == Ordering::Less
            || value.try_cmp_with_work(&half_period, work)? != Ordering::Less
        {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "exact retained longitude is outside its canonical native window".into(),
            )
            .into());
        }
        Ok(Self(value))
    }

    #[cfg(test)]
    pub(crate) const fn rational(&self) -> &ExactRational {
        &self.0
    }

    pub(crate) fn round_native(
        axis: crate::geographic::PeriodicAxis,
        native: &Self,
    ) -> crate::error::Result<(crate::geographic::CanonicalLongitude, i8)> {
        let mut work = crate::work::Unmetered;
        Self::round_native_with_work(axis, native, &mut work)
    }

    pub(crate) fn round_native_with_work(
        axis: crate::geographic::PeriodicAxis,
        native: &Self,
        work: &mut impl crate::work::Work,
    ) -> crate::error::Result<(crate::geographic::CanonicalLongitude, i8)> {
        let (native, adjustment) =
            round_native_site_with_work(&native.0, axis.period(), axis.half_period(), work)?;
        let canonical = axis.canonicalize(native).map_err(|error| {
            crate::geometry::GeometryErrorKind::Invalid(error.to_string().into())
        })?;
        Ok((canonical, adjustment))
    }

    #[cfg(test)]
    /// Round a mathematical site for publication without losing the exact
    /// site used to choose its native-window quotient.
    pub(crate) fn round_site(
        axis: crate::geographic::PeriodicAxis,
        site: &crate::geographic::RetainedLongitudeSite<Self>,
    ) -> crate::error::Result<crate::geographic::RetainedLongitudeSite> {
        let (canonical, adjustment) = Self::round_native(axis, site.native())?;
        let periods = site
            .periods()
            .checked_add(i64::from(adjustment))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(crate::geographic::RetainedLongitudeSite::new(
            canonical, periods,
        ))
    }
}

impl Sealed for ExactNativeLongitude {}

impl crate::geographic::RetainedNativeLongitude for ExactNativeLongitude {}

#[cfg(test)]
pub(crate) fn retained_exact_native_site(
    axis: crate::geographic::PeriodicAxis,
    longitude: &ExactRational,
) -> crate::error::Result<crate::geographic::RetainedLongitudeSite<ExactNativeLongitude>> {
    let mut work = crate::work::Unmetered;
    retained_exact_native_site_with_work(axis, longitude, &mut work)
}

pub(crate) fn retained_exact_native_site_with_work(
    axis: crate::geographic::PeriodicAxis,
    longitude: &ExactRational,
    work: &mut impl crate::work::Work,
) -> crate::error::Result<crate::geographic::RetainedLongitudeSite<ExactNativeLongitude>> {
    let (native, lift) = split_rational_native_site(longitude, axis.period(), axis.half_period())?;
    let periods = exact_integer_i64(&lift)?;
    Ok(crate::geographic::RetainedLongitudeSite::new(
        ExactNativeLongitude::from_rational_with_work(axis, native.into_owned(), work)?,
        periods,
    ))
}

#[cfg(test)]
pub(crate) fn round_native_longitude(
    axis: crate::geographic::PeriodicAxis,
    longitude: &ExactRational,
) -> crate::error::Result<f64> {
    let mut work = crate::work::Unmetered;
    round_native_longitude_with_work(axis, longitude, &mut work)
}

pub(crate) fn round_native_longitude_with_work(
    axis: crate::geographic::PeriodicAxis,
    longitude: &ExactRational,
    work: &mut impl crate::work::Work,
) -> crate::error::Result<f64> {
    let (native, _) =
        split_rational_native_site_with_work(longitude, axis.period(), axis.half_period(), work)?;
    let rounded = native.round_with_work(work)?;
    let (rounded, _) =
        fold_rounded_native_site_with_work(rounded, axis.period(), axis.half_period(), work)?;
    Ok(rounded)
}

fn round_native_site(
    native: &ExactRational,
    period: f64,
    half_period: f64,
) -> crate::error::Result<(f64, i8)> {
    let mut work = crate::work::Unmetered;
    round_native_site_with_work(native, period, half_period, &mut work)
}

fn round_native_site_with_work(
    native: &ExactRational,
    period: f64,
    half_period: f64,
    work: &mut impl crate::work::Work,
) -> crate::error::Result<(f64, i8)> {
    fold_rounded_native_site_with_work(native.round_with_work(work)?, period, half_period, work)
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RoundedNativeSite {
    pub native: f64,
    pub periods: i64,
}

pub(crate) fn rounded_native_neighbors_with_work(
    axis: crate::geographic::PeriodicAxis,
    longitude: &ExactRational,
    work: &mut impl crate::work::Work,
) -> crate::error::Result<[RoundedNativeSite; 3]> {
    let exact = retained_exact_native_site_with_work(axis, longitude, work)?;
    let (native, initial_adjustment) =
        ExactNativeLongitude::round_native_with_work(axis, exact.native(), work)?;
    let native = native.get();
    let base_periods = exact
        .periods()
        .checked_add(i64::from(initial_adjustment))
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let period = axis.period();
    let half_period = axis.half_period();
    let mut round_candidate = |candidate: f64| -> crate::error::Result<RoundedNativeSite> {
        let (native, adjustment) =
            fold_rounded_native_site_with_work(candidate, period, half_period, work)?;
        let periods = base_periods
            .checked_add(i64::from(adjustment))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let native = axis
            .canonicalize(native)
            .map_err(|error| crate::geometry::GeometryErrorKind::Invalid(error.to_string().into()))?
            .get();
        Ok(RoundedNativeSite { native, periods })
    };
    Ok([
        round_candidate(native.next_down())?,
        round_candidate(native)?,
        round_candidate(native.next_up())?,
    ])
}

/// Form the nearest original-source lift by an integer period count and round once.
#[expect(
    clippy::panic,
    reason = "the caller admits finite endpoints and PeriodicAxis supplies a positive finite period; output-range failure maps to signed infinity"
)]
pub(crate) fn nearest_periodic_alias(source: f64, reference: f64, period: f64) -> f64 {
    use crate::geometry::round_ratio;

    debug_assert!(source.is_finite() && reference.is_finite());
    debug_assert!(period.is_finite() && period > 0.0);
    let source = ExactDyadic::from_f64(source);
    let difference = ExactDyadic::from_f64(reference)
        .try_subtract_ref(&source)
        .expect("finite binary64 longitude differences fit the exact exponent range");
    let period = ExactDyadic::from_f64(period);
    let periods = difference
        .nearest_integer_quotient_ties_to_zero(&period)
        .expect("finite binary64 period quotients fit the exact exponent range");
    let translation = periods
        .try_product(&period)
        .expect("finite binary64 period lifts fit the exact exponent range");
    let lifted = source
        .try_add(translation)
        .expect("finite binary64 period lifts fit the exact exponent range");
    match round_ratio(&lifted, &ExactDyadic::one()) {
        Ok(value) => value,
        Err(error)
            if matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(
                    crate::geometry::GeometryErrorKind::ExactCoordinateOutOfRange
                )
            ) =>
        {
            if lifted.negative {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            }
        },
        Err(error) => panic!("finite binary64 period lift could not be rounded: {error}"),
    }
}

/// Evaluate one retained binary64-parameterized edge in its canonical native
/// window, retaining the integral site lift separately from the rounded site.
#[expect(
    clippy::float_cmp,
    reason = "zero and one are exact source-edge parameters with endpoint-record semantics"
)]
pub(crate) fn retained_native_site(
    start: f64,
    end: f64,
    period: f64,
    half_period: f64,
    periods: i64,
    parameter: f64,
) -> crate::error::Result<(f64, i64)> {
    if !(start.is_finite()
        && end.is_finite()
        && period.is_finite()
        && half_period.is_finite()
        && parameter.is_finite()
        && (0.0..=1.0).contains(&parameter))
    {
        return Err(crate::geometry::GeometryErrorKind::message(
            "retained chart position is outside its finite edge domain",
        ));
    }
    if parameter == 0.0 {
        return Ok((if start == 0.0 { 0.0 } else { start }, 0));
    }
    if parameter == 1.0 {
        return Ok((if end == 0.0 { 0.0 } else { end }, periods));
    }
    if periods == 0 {
        let native = crate::geometry::interpolate_subdivision_ordinate(start, end, parameter);
        return Ok((if native == 0.0 { 0.0 } else { native }, 0));
    }

    let start_exact = ExactRational::from_f64(start);
    let end_exact = ExactRational::from_f64(end);
    let period_rational = ExactRational::from_f64(period);
    let period_lift = ExactRational::from_dyadic(ExactDyadic::from_i64(periods));
    let delta = end_exact
        .try_subtract(&start_exact)?
        .try_add(&period_rational.try_product(&period_lift)?)?;
    let position = start_exact.try_add(&delta.try_product(&ExactRational::from_f64(parameter))?)?;
    let (native, lift) = split_rational_native_site(&position, period, half_period)?;
    let (rounded, adjustment) = round_native_site(&native, period, half_period)?;
    let lift = lift.try_add(ExactDyadic::from_i64(i64::from(adjustment)))?;
    Ok((rounded, exact_integer_i64(&lift)?))
}

/// Exact chart longitude for a canonical native site plus its integral lift.
///
/// The binary64 lift is useful for numeric candidate construction only; exact
/// ordering and predicates retain this dyadic sum instead.
pub(crate) fn retained_site_longitude(
    axis: crate::geographic::PeriodicAxis,
    site: crate::geographic::RetainedLongitudeSite,
) -> crate::error::Result<ExactDyadic> {
    let native = ExactDyadic::from_f64(site.native().get());
    if site.periods() == 0 {
        return Ok(native);
    }
    let lift = ExactDyadic::from_i64(site.periods());
    let offset = ExactDyadic::from_f64(axis.period()).try_product(&lift)?;
    native.try_add(offset)
}

pub(crate) fn retained_site_orientation(
    axis: crate::geographic::PeriodicAxis,
    points: &[(crate::geographic::RetainedLongitudeSite, f64); 3],
) -> crate::error::Result<ExactSign> {
    let [(a_site, ay), (b_site, by), (c_site, cy)] = points;
    let ax = retained_site_longitude(axis, *a_site)?;
    let bx = retained_site_longitude(axis, *b_site)?;
    let cx = retained_site_longitude(axis, *c_site)?;
    let ay = ExactDyadic::from_f64(*ay);
    let by = ExactDyadic::from_f64(*by);
    let cy = ExactDyadic::from_f64(*cy);
    let bax = bx.try_subtract(ax.clone())?;
    let bay = by.try_subtract(ay.clone())?;
    let cax = cx.try_subtract(ax)?;
    let cay = cy.try_subtract(ay)?;
    Ok(bax
        .try_product(&cay)?
        .try_subtract(bay.try_product(&cax)?)?
        .sign())
}

pub(crate) fn retained_site_distance_order(
    axis: crate::geographic::PeriodicAxis,
    left: (crate::geographic::RetainedLongitudeSite, f64),
    right: (crate::geographic::RetainedLongitudeSite, f64),
    center: crate::geometry::Point,
) -> crate::error::Result<std::cmp::Ordering> {
    let center_x = ExactDyadic::from_f64(center.x);
    let center_y = ExactDyadic::from_f64(center.y);
    let squared_distance =
        |(longitude, latitude): (crate::geographic::RetainedLongitudeSite, f64)| {
            let dx = retained_site_longitude(axis, longitude)?.try_subtract(center_x.clone())?;
            let dy = ExactDyadic::from_f64(latitude).try_subtract(center_y.clone())?;
            dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)
        };
    Ok(squared_distance(left)?.cmp(&squared_distance(right)?))
}

/// Compare exact products of three finite binary64 factors.
///
/// The fixed factor count is enough for cross-multiplied unit ratios without
/// rounding an intermediate PM value.
pub(crate) fn compare_three_f64_products(left: [f64; 3], right: [f64; 3]) -> std::cmp::Ordering {
    let product = |factors: [f64; 3]| {
        factors
            .into_iter()
            .fold(ExactDyadic::one(), |current, factor| {
                current
                    .try_product(&ExactDyadic::from_f64(factor))
                    .expect("three finite binary64 factors have representable exponents")
            })
    };
    product(left).cmp(&product(right))
}

fn round_retained_source_sheet_value(value: &ExactDyadic) -> crate::error::Result<f64> {
    use crate::geometry::round_ratio;

    match round_ratio(value, &ExactDyadic::one()) {
        Ok(value) => Ok(value),
        Err(error)
            if matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(
                    crate::geometry::GeometryErrorKind::ExactCoordinateOutOfRange
                )
            ) =>
        {
            Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
                "computed edge",
            ))
        },
        Err(error) => Err(error),
    }
}

/// Publish one retained chart site on the source's absolute longitude sheet.
///
/// Metric consumers use the edge's relative retained lift to avoid
/// cancellation at large source aliases. A source-aligned output adds the
/// absolute sheet only at this publication boundary, in exact arithmetic,
/// then rounds the complete coordinate once.
pub(crate) fn retained_source_sheet_site(
    source_start: f64,
    edge: crate::geographic::RetainedChartDisplacement,
    axis: crate::geographic::PeriodicAxis,
    parameter: f64,
) -> crate::error::Result<f64> {
    let period = axis.period();
    if !(source_start.is_finite()
        && period.is_finite()
        && period > 0.0
        && parameter.is_finite()
        && (0.0..=1.0).contains(&parameter))
    {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "computed edge",
        ));
    }
    let [start, _] = *edge.endpoints();
    let start = ExactDyadic::from_f64(start.get());
    let period = ExactDyadic::from_f64(period);
    let source_start = ExactDyadic::from_f64(source_start);
    let parameter = ExactDyadic::from_f64(parameter);
    let source_sheet = source_start
        .try_subtract(start.clone())?
        .nearest_integer_quotient_ties_to_zero(&period)?;
    let relative_displacement = retained_edge_displacement(edge, axis)?;
    let relative_site = start.try_add(relative_displacement.try_product(&parameter)?)?;
    let absolute_site = relative_site.try_add(period.try_product(&source_sheet)?)?;
    round_retained_source_sheet_value(&absolute_site)
}

pub(super) fn split_rational_native_site(
    position: &ExactRational,
    period: f64,
    half_period: f64,
) -> crate::error::Result<(std::borrow::Cow<'_, ExactRational>, ExactDyadic)> {
    let mut work = crate::work::Unmetered;
    split_rational_native_site_with_work(position, period, half_period, &mut work)
}

pub(crate) fn split_rational_native_site_with_work<'position>(
    position: &'position ExactRational,
    period: f64,
    half_period: f64,
    work: &mut impl crate::work::Work,
) -> crate::error::Result<(std::borrow::Cow<'position, ExactRational>, ExactDyadic)> {
    use std::cmp::Ordering;

    if !(period.is_finite() && period > 0.0 && half_period.is_finite() && half_period > 0.0) {
        return Err(crate::geometry::GeometryErrorKind::message(
            "retained chart period must be positive and finite",
        ));
    }
    let half = ExactRational::from_f64(half_period);
    let negative_half = ExactRational::from_f64(-half_period);
    if position.try_cmp_with_work(&negative_half, work)? != Ordering::Less
        && position.try_cmp_with_work(&half, work)? == Ordering::Less
    {
        return Ok((std::borrow::Cow::Borrowed(position), ExactDyadic::zero()));
    }
    let (numerator, denominator) = position.parts();
    let quotient_denominator =
        denominator.try_product_with_work(&ExactDyadic::from_f64(period), work)?;
    let mut lift =
        numerator.nearest_integer_quotient_ties_to_zero_with_work(&quotient_denominator, work)?;
    let exact_period = ExactRational::from_f64(period);
    let native = position.try_subtract_with_work(
        &exact_period.try_product_with_work(
            &ExactRational::from_dyadic(lift.try_clone_with_work(work)?),
            work,
        )?,
        work,
    )?;
    let (native, lift) = if native.try_cmp_with_work(&half, work)? != Ordering::Less {
        lift = lift.try_add_with_work(ExactDyadic::one(), work)?;
        (native.try_subtract_with_work(&exact_period, work)?, lift)
    } else if native.try_cmp_with_work(&negative_half, work)? == Ordering::Less {
        lift = lift.try_subtract_with_work(ExactDyadic::one(), work)?;
        (native.try_add_with_work(&exact_period, work)?, lift)
    } else {
        (native, lift)
    };
    Ok((std::borrow::Cow::Owned(native), lift))
}

fn fold_rounded_native_site_with_work(
    native: f64,
    period: f64,
    half_period: f64,
    work: &mut impl crate::work::Work,
) -> crate::error::Result<(f64, i8)> {
    use crate::geometry::round_ratio_with_work;

    let (folded, adjustment) = if native >= half_period {
        (
            ExactDyadic::from_f64(native)
                .try_subtract_ref_with_work(&ExactDyadic::from_f64(period), work)?,
            1,
        )
    } else if native < -half_period {
        (
            ExactDyadic::from_f64(native).try_add_with_work(ExactDyadic::from_f64(period), work)?,
            -1,
        )
    } else {
        return Ok((if native == 0.0 { 0.0 } else { native }, 0));
    };
    let folded = round_ratio_with_work(&folded, &ExactDyadic::one(), work)?;
    Ok((if folded == 0.0 { 0.0 } else { folded }, adjustment))
}

pub(crate) fn retained_lifted_native(
    native: f64,
    period: f64,
    periods: i64,
) -> crate::error::Result<f64> {
    use crate::geometry::round_ratio;

    if !(native.is_finite() && period.is_finite() && period > 0.0) {
        return Err(crate::geometry::GeometryErrorKind::message(
            "retained native site and period must be finite",
        ));
    }
    if periods == 0 {
        return Ok(native);
    }
    let translated = ExactDyadic::from_f64(native)
        .try_add(ExactDyadic::from_f64(period).try_product(&ExactDyadic::from_i64(periods))?)?;
    round_ratio(&translated, &ExactDyadic::one())
}

pub(crate) fn retained_delta_native(start: f64, end: f64, period: f64, periods: i64) -> f64 {
    if periods == 0 {
        return end - start;
    }
    let mut delta = product_sum::ExactSum::zero();
    delta.add_product(end, 1.0);
    delta.subtract_product(start, 1.0);
    delta.add_scaled_product(period, 1.0, periods, false);
    delta.round_to_binary64()
}

pub(crate) fn retained_delta_scaled(
    start: f64,
    end: f64,
    period: f64,
    periods: i64,
    scale: f64,
) -> f64 {
    let mut delta = product_sum::ExactSum::zero();
    delta.add_product(end, scale);
    delta.subtract_product(start, scale);
    delta.add_scaled_product(period, scale, periods, false);
    delta.round_to_binary64()
}

pub(super) fn exact_integer_i64(value: &ExactDyadic) -> crate::error::Result<i64> {
    if value.is_zero() {
        return Ok(0);
    }
    if value.exponent < 0 {
        return Err(crate::geometry::GeometryErrorKind::message(
            "retained periodic lift is not integral",
        ));
    }
    let magnitude = value
        .small_magnitude()
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let shift =
        u32::try_from(value.exponent).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    if shift >= u128::BITS || magnitude > (u128::MAX >> shift) {
        return Err(crate::error::ResourceError::SizeOverflow.into());
    }
    let magnitude = magnitude << shift;
    if value.sign() == ExactSign::Negative {
        let minimum = 1_u128 << 63;
        if magnitude == minimum {
            Ok(i64::MIN)
        } else {
            i64::try_from(magnitude)
                .map(i64::wrapping_neg)
                .map_err(|_| crate::error::ResourceError::SizeOverflow.into())
        }
    } else {
        i64::try_from(magnitude).map_err(|_| crate::error::ResourceError::SizeOverflow.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::GeographicFrame;

    #[test]
    fn exact_integer_i64_admits_signed_bounds_and_rejects_fractional_overflow() {
        for (name, value, expected) in [
            ("zero", ExactDyadic::zero(), 0),
            ("positive one", ExactDyadic::from_i64(1), 1),
            ("negative one", ExactDyadic::from_i64(-1), -1),
            ("i64::MAX", ExactDyadic::from_i64(i64::MAX), i64::MAX),
            ("i64::MIN", ExactDyadic::from_i64(i64::MIN), i64::MIN),
        ] {
            assert_eq!(exact_integer_i64(&value).unwrap(), expected, "{name}");
        }

        for (name, value) in [
            ("positive 2^63", ExactDyadic::from_parts(false, vec![1], 63)),
            (
                "negative magnitude 2^63 + 1",
                ExactDyadic::from_parts(true, vec![0x8000_0000_0000_0001], 0),
            ),
        ] {
            assert!(
                matches!(
                    exact_integer_i64(&value).unwrap_err().kind(),
                    crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
                ),
                "{name}"
            );
        }

        let fractional = ExactDyadic::from_parts(false, vec![1], -1);
        assert!(matches!(
            exact_integer_i64(&fractional).unwrap_err().kind(),
            crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::Invalid(message)
            ) if message.as_ref() == "retained periodic lift is not integral"
        ));
    }
    #[test]
    fn retained_native_sites_fold_rounded_seams_and_preserve_authentic_endpoints() {
        let axis = GeographicFrame::crs84().longitude();
        let start = 180.0_f64.next_down();
        let end = -180.0;
        let edge = axis
            .retained_native_endpoints(start, end, 1)
            .expect("the explicit producer branch is retained");

        let rounded_positive_seam = axis.native_site_at(&edge, 0.75).unwrap();
        assert_eq!(rounded_positive_seam.native().get(), -180.0);
        assert_eq!(rounded_positive_seam.periods(), 1);
        assert!(
            (-180.0..180.0).contains(&rounded_positive_seam.native().get()),
            "published native site must be in the canonical half-open window"
        );
        assert_eq!(
            axis.canonicalize(rounded_positive_seam.native().get())
                .unwrap(),
            *rounded_positive_seam.native()
        );
        assert_eq!(
            axis.lifted_site_longitude(rounded_positive_seam).unwrap(),
            180.0
        );

        let at_start = axis.native_site_at(&edge, 0.0).unwrap();
        let at_end = axis.native_site_at(&edge, 1.0).unwrap();
        assert_eq!(at_start.native().get().to_bits(), start.to_bits());
        assert_eq!(at_start.periods(), 0);
        assert_eq!(at_end.native().get().to_bits(), end.to_bits());
        assert_eq!(at_end.periods(), 1);

        let reversed = axis
            .retained_native_endpoints(end, start, -1)
            .expect("the reversed producer branch is retained");
        let rounded_negative_seam = axis.native_site_at(&reversed, 0.25).unwrap();
        assert_eq!(rounded_negative_seam.native().get(), -180.0);
        assert_eq!(rounded_negative_seam.periods(), 0);
        assert_eq!(
            axis.canonicalize(rounded_negative_seam.native().get())
                .unwrap(),
            *rounded_negative_seam.native()
        );
    }
}
