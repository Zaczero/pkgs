//! Error-free transformations and double-double arithmetic: the one home for
//! `two_sum`, `two_product`, and the `dd_*` kernels every exact predicate and
//! expansion builds on, and for [`bounded`] certificates.
//!
//! Products take their residual from a fused multiply-add. At the x86-64-v2
//! baseline `mul_add` is a libm call, but glibc dispatches it to the hardware
//! FMA instruction where the CPU has one, and call plus instruction retire
//! fewer instructions than a Dekker split with the rescaling its splitter
//! needs at large magnitudes. [`two_product_exact`] declines a product whose
//! residual might not be representable; [`two_product`] serves filtered
//! predicates, whose error bound covers a rounded subnormal residual.
//! [`DekkerParts`] stores one checked coefficient split for hot kernels
//! compiled to baselines where scalar `mul_add` is not a hardware instruction.

pub(crate) mod bounded;
pub(crate) mod product_sum;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(in crate::geometry) struct ExactNativeLongitude(
    crate::geometry::tessellation::exact::ExactRational,
);

#[cfg(test)]
impl ExactNativeLongitude {
    pub(in crate::geometry) fn from_rational(
        axis: crate::geographic::PeriodicAxis,
        value: crate::geometry::tessellation::exact::ExactRational,
    ) -> crate::error::Result<Self> {
        use std::cmp::Ordering;

        use crate::geometry::tessellation::exact::ExactRational;

        let half_period = ExactRational::from_f64(axis.half_period());
        let negative_half_period = ExactRational::from_f64(-axis.half_period());
        if value.try_cmp(&negative_half_period)? == Ordering::Less
            || value.try_cmp(&half_period)? != Ordering::Less
        {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "exact retained longitude is outside its canonical native window".into(),
            )
            .into());
        }
        Ok(Self(value))
    }

    pub(in crate::geometry) fn rational(
        &self,
    ) -> &crate::geometry::tessellation::exact::ExactRational {
        &self.0
    }

    /// Round a mathematical site for publication without losing the exact
    /// site used to choose its native-window quotient.
    pub(in crate::geometry) fn round_site(
        axis: crate::geographic::PeriodicAxis,
        site: &crate::geographic::RetainedLongitudeSite<Self>,
    ) -> crate::error::Result<crate::geographic::RetainedLongitudeSite> {
        let (native, adjustment) =
            round_native_site(&site.native().0, axis.period(), axis.half_period())?;
        let periods = site
            .periods()
            .checked_add(i64::from(adjustment))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let canonical = axis.canonicalize(native).map_err(|error| {
            crate::geometry::GeometryErrorKind::Invalid(error.to_string().into())
        })?;
        Ok(crate::geographic::RetainedLongitudeSite::new(
            canonical, periods,
        ))
    }
}

#[cfg(test)]
pub(in crate::geometry) fn retained_exact_native_site(
    axis: crate::geographic::PeriodicAxis,
    longitude: &crate::geometry::tessellation::exact::ExactRational,
) -> crate::error::Result<crate::geographic::RetainedLongitudeSite<ExactNativeLongitude>> {
    let (native, lift) = split_rational_native_site(longitude, axis.period(), axis.half_period())?;
    let periods = exact_integer_i64(&lift)?;
    Ok(crate::geographic::RetainedLongitudeSite::new(
        ExactNativeLongitude(native.into_owned()),
        periods,
    ))
}

#[cfg(test)]
impl crate::geographic::retained_native_private::Sealed for ExactNativeLongitude {}

#[cfg(test)]
impl crate::geographic::RetainedNativeLongitude for ExactNativeLongitude {}

pub(in crate::geometry) fn round_native_longitude(
    axis: crate::geographic::PeriodicAxis,
    longitude: &crate::geometry::tessellation::exact::ExactRational,
) -> crate::error::Result<f64> {
    let (native, _) = split_rational_native_site(longitude, axis.period(), axis.half_period())?;
    let (rounded, _) = round_native_site(&native, axis.period(), axis.half_period())?;
    Ok(rounded)
}

fn round_native_site(
    native: &crate::geometry::tessellation::exact::ExactRational,
    period: f64,
    half_period: f64,
) -> crate::error::Result<(f64, i8)> {
    fold_rounded_native_site(native.round()?, period, half_period)
}

/// Compare exact products of three finite binary64 factors.
///
/// The fixed factor count is enough for cross-multiplied unit ratios without
/// rounding an intermediate PM value.
pub(crate) fn compare_three_f64_products(left: [f64; 3], right: [f64; 3]) -> std::cmp::Ordering {
    use crate::geometry::tessellation::exact::ExactDyadic;

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

/// Form the nearest original-source lift by an integer period count and round once.
pub(crate) fn nearest_periodic_alias(source: f64, reference: f64, period: f64) -> f64 {
    use crate::geometry::tessellation::exact::{ExactDyadic, round_ratio};

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
pub(crate) fn retained_native_site(
    start: f64,
    end: f64,
    period: f64,
    half_period: f64,
    periods: i64,
    parameter: f64,
) -> crate::error::Result<(f64, i64)> {
    use crate::geometry::tessellation::exact::ExactDyadic;

    if !(start.is_finite()
        && end.is_finite()
        && period.is_finite()
        && half_period.is_finite()
        && parameter.is_finite()
        && (0.0..=1.0).contains(&parameter))
    {
        return Err(crate::geometry::GeometryErrorKind::message(
            "retained chart position is outside its finite edge domain",
        )
        .into());
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

    let start_exact = crate::geometry::tessellation::exact::ExactRational::from_f64(start);
    let end_exact = crate::geometry::tessellation::exact::ExactRational::from_f64(end);
    let period_rational = crate::geometry::tessellation::exact::ExactRational::from_f64(period);
    let period_lift = crate::geometry::tessellation::exact::ExactRational::from_dyadic(
        ExactDyadic::from_i64(periods),
    );
    let delta = end_exact
        .try_subtract(&start_exact)?
        .try_add(&period_rational.try_product(&period_lift)?)?;
    let position = start_exact.try_add(&delta.try_product(
        &crate::geometry::tessellation::exact::ExactRational::from_f64(parameter),
    )?)?;
    let (native, lift) = split_rational_native_site(&position, period, half_period)?;
    let (rounded, adjustment) = round_native_site(&native, period, half_period)?;
    let lift = lift.try_add(ExactDyadic::from_i64(i64::from(adjustment)))?;
    Ok((rounded, exact_integer_i64(&lift)?))
}

fn retained_edge_displacement(
    edge: crate::geographic::RetainedChartDisplacement,
    axis: crate::geographic::PeriodicAxis,
) -> crate::error::Result<crate::geometry::tessellation::exact::ExactDyadic> {
    use crate::geometry::tessellation::exact::ExactDyadic;

    let [start, end] = *edge.endpoints();
    ExactDyadic::from_f64(end.get())
        .try_subtract(ExactDyadic::from_f64(start.get()))?
        .try_add(
            ExactDyadic::from_f64(axis.period())
                .try_product(&ExactDyadic::from_i64(edge.periods()))?,
        )
}

fn round_retained_source_sheet_value(
    value: &crate::geometry::tessellation::exact::ExactDyadic,
) -> crate::error::Result<f64> {
    use crate::geometry::tessellation::exact::{ExactDyadic, round_ratio};

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
    use crate::geometry::tessellation::exact::ExactDyadic;

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

/// Check that the written chart step preserves the exact source interval's
/// direction and branch class.
///
/// The displacement remains exact through classification. Comparing it to the
/// subtraction of rounded output coordinates would reject correctly rounded
/// source sites whenever those two rounding operations do not commute.
#[expect(
    clippy::float_cmp,
    reason = "zero and nonzero retained branch classes are discrete"
)]
pub(crate) fn retained_source_interval_reads_as(
    edge: crate::geographic::RetainedChartDisplacement,
    axis: crate::geographic::PeriodicAxis,
    from: f64,
    to: f64,
    readback: crate::geographic::ChartStep,
) -> crate::error::Result<bool> {
    use std::cmp::Ordering;

    use crate::geographic::{ChartStep, EdgeDirection, TurnDirection};
    use crate::geometry::tessellation::exact::ExactDyadic;

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
    let delta = retained_edge_displacement(edge, axis)?.try_product(&parameter_delta)?;
    if delta.is_zero() {
        return Ok(matches!(
            readback,
            ChartStep::Shortest(EdgeDirection::Minor(value)) if value == 0.0
        ));
    }

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
    Ok(match readback {
        ChartStep::Shortest(EdgeDirection::Minor(value)) => {
            magnitude.cmp(&half_period) != Ordering::Greater
                && value != 0.0
                && value.is_sign_negative() == negative
        },
        ChartStep::Shortest(EdgeDirection::HalfTurn(direction)) => {
            magnitude.cmp(&half_period) == Ordering::Equal && direction_matches(direction)
        },
        ChartStep::Circle(direction) => {
            magnitude.cmp(&period) == Ordering::Equal && direction_matches(direction)
        },
    })
}

fn exact_nonnegative_usize(
    value: &crate::geometry::tessellation::exact::ExactDyadic,
) -> crate::error::Result<usize> {
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

    use crate::geometry::tessellation::exact::ExactDyadic;

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

fn split_rational_native_site(
    position: &crate::geometry::tessellation::exact::ExactRational,
    period: f64,
    half_period: f64,
) -> crate::error::Result<(
    std::borrow::Cow<'_, crate::geometry::tessellation::exact::ExactRational>,
    crate::geometry::tessellation::exact::ExactDyadic,
)> {
    use std::cmp::Ordering;

    use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};

    if !(period.is_finite() && period > 0.0 && half_period.is_finite() && half_period > 0.0) {
        return Err(crate::geometry::GeometryErrorKind::message(
            "retained chart period must be positive and finite",
        )
        .into());
    }
    let half = ExactRational::from_f64(half_period);
    let negative_half = ExactRational::from_f64(-half_period);
    if position.try_cmp(&negative_half)? != Ordering::Less
        && position.try_cmp(&half)? == Ordering::Less
    {
        return Ok((std::borrow::Cow::Borrowed(position), ExactDyadic::zero()));
    }
    let (numerator, denominator) = position.parts();
    let quotient_denominator = denominator.try_product(&ExactDyadic::from_f64(period))?;
    let mut lift = numerator.nearest_integer_quotient_ties_to_zero(&quotient_denominator)?;
    let exact_period = ExactRational::from_f64(period);
    let native = position
        .try_subtract(&exact_period.try_product(&ExactRational::from_dyadic(lift.clone()))?)?;
    let (native, lift) = if native.try_cmp(&half)? != Ordering::Less {
        lift = lift.try_add(ExactDyadic::one())?;
        (native.try_subtract(&exact_period)?, lift)
    } else if native.try_cmp(&negative_half)? == Ordering::Less {
        lift = lift.try_subtract(ExactDyadic::one())?;
        (native.try_add(&exact_period)?, lift)
    } else {
        (native, lift)
    };
    Ok((std::borrow::Cow::Owned(native), lift))
}

fn fold_rounded_native_site(
    native: f64,
    period: f64,
    half_period: f64,
) -> crate::error::Result<(f64, i8)> {
    use crate::geometry::tessellation::exact::{ExactDyadic, round_ratio};

    let (folded, adjustment) = if native >= half_period {
        (
            ExactDyadic::from_f64(native).try_subtract_ref(&ExactDyadic::from_f64(period))?,
            1,
        )
    } else if native < -half_period {
        (
            ExactDyadic::from_f64(native).try_add(ExactDyadic::from_f64(period))?,
            -1,
        )
    } else {
        return Ok((if native == 0.0 { 0.0 } else { native }, 0));
    };
    let folded = round_ratio(&folded, &ExactDyadic::one())?;
    Ok((if folded == 0.0 { 0.0 } else { folded }, adjustment))
}

pub(crate) fn retained_lifted_native(
    native: f64,
    period: f64,
    periods: i64,
) -> crate::error::Result<f64> {
    use crate::geometry::tessellation::exact::{ExactDyadic, round_ratio};

    if !(native.is_finite() && period.is_finite() && period > 0.0) {
        return Err(crate::geometry::GeometryErrorKind::message(
            "retained native site and period must be finite",
        )
        .into());
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

fn exact_integer_i64(
    value: &crate::geometry::tessellation::exact::ExactDyadic,
) -> crate::error::Result<i64> {
    if value.is_zero() {
        return Ok(0);
    }
    if value.exponent < 0 {
        return Err(crate::geometry::GeometryErrorKind::message(
            "retained periodic lift is not integral",
        )
        .into());
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
                .map(|magnitude| -magnitude)
                .map_err(|_| crate::error::ResourceError::SizeOverflow.into())
        }
    } else {
        i64::try_from(magnitude).map_err(|_| crate::error::ResourceError::SizeOverflow.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) enum ExactSign {
    Negative,
    Zero,
    Positive,
}

/// Error-free sum (Knuth two-sum): `head + tail == left + right` exactly
/// whenever the rounded head is finite.  Callers that need a fallible lane
/// check that head before using the pair; the shared transform itself keeps
/// the infallible EFT contract used by the segment and rectangle kernels.
pub(crate) fn two_sum(left: f64, right: f64) -> (f64, f64) {
    let sum = left + right;
    let right_virtual = sum - left;
    (
        sum,
        (left - (sum - right_virtual)) + (right - right_virtual),
    )
}

pub(crate) fn fast_two_sum(left: f64, right: f64) -> (f64, f64) {
    let sum = left + right;
    (sum, right - (sum - left))
}

/// Lowest rounded-product exponent whose 106-bit exact product still fits the
/// binary64 subnormal grid in its residual.
const MIN_EXACT_PRODUCT_EXPONENT: i32 = -969;

/// Error-free product for finite, non-underflowing binary64 products: the
/// fused residual. Below the non-underflowing range the residual is rounded
/// to the subnormal grid, within half a subnormal ulp.
pub(crate) fn two_product(lhs: f64, rhs: f64) -> (f64, f64) {
    let head = lhs * rhs;
    if !head.is_finite() || head == 0.0 {
        return (head, 0.0);
    }
    (head, lhs.mul_add(rhs, -head))
}

const DEKKER_SPLITTER: f64 = 134_217_729.0;
const DEKKER_PRODUCT_EXPONENT_MIN: i32 = -850;
const DEKKER_PRODUCT_EXPONENT_MAX: i32 = 850;
const DEKKER_SPLIT_EXPONENT_MAX: i32 = 995;

#[derive(Clone, Copy)]
pub(crate) struct DekkerParts {
    value: f64,
    high: f64,
    low: f64,
    exponent: i32,
}

impl DekkerParts {
    pub(crate) const fn zero() -> Self {
        Self {
            value: 0.0,
            high: 0.0,
            low: 0.0,
            exponent: 0,
        }
    }

    pub(crate) fn checked(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        if binary_zero(value) {
            return Some(Self {
                value,
                high: value,
                low: 0.0,
                exponent: 0,
            });
        }
        let (high, low) = dekker_split(value)?;
        Some(Self {
            value,
            high,
            low,
            exponent: floor_binary_exponent(value.abs()),
        })
    }

    /// Split a finite input whose exponent was admitted with its entire row.
    pub(crate) fn admitted(value: f64) -> Self {
        if binary_zero(value) {
            return Self {
                value,
                high: value,
                low: 0.0,
                exponent: 0,
            };
        }
        let product = DEKKER_SPLITTER * value;
        let large = product - value;
        let high = product - large;
        let low = value - high;
        Self {
            value,
            high,
            low,
            exponent: 0,
        }
    }

    pub(crate) const fn exponent(self) -> i32 {
        self.exponent
    }

    pub(crate) const fn value(self) -> f64 {
        self.value
    }

    pub(crate) const fn is_zero(self) -> bool {
        binary_zero(self.value)
    }

    pub(crate) fn negated(self) -> Self {
        Self {
            value: -self.value,
            high: -self.high,
            low: -self.low,
            exponent: self.exponent,
        }
    }
}

/// Whether the binary64 encoding is either sign of zero.
pub(crate) const fn binary_zero(value: f64) -> bool {
    value.to_bits().trailing_zeros() >= 63
}

/// Whether an intermediate is finite and normal, or exactly zero.
pub(crate) const fn normal_or_zero(value: f64) -> bool {
    binary_zero(value) || value.is_normal()
}

/// One rounded sum that declines overflow and subnormal results.
pub(crate) fn checked_normal_sum(lhs: f64, rhs: f64) -> Option<f64> {
    let sum = lhs + rhs;
    normal_or_zero(sum).then_some(sum)
}

fn dekker_product(lhs: f64, rhs: f64) -> Option<f64> {
    if binary_zero(lhs) || binary_zero(rhs) {
        let product = lhs * rhs;
        return product.is_finite().then_some(product);
    }
    let product = lhs * rhs;
    product.is_normal().then_some(product)
}

fn dekker_split(value: f64) -> Option<(f64, f64)> {
    if floor_binary_exponent(value.abs()) > DEKKER_SPLIT_EXPONENT_MAX {
        return None;
    }
    let product = dekker_product(DEKKER_SPLITTER, value)?;
    let large = checked_normal_sum(product, -value)?;
    let high = checked_normal_sum(product, -large)?;
    let low = checked_normal_sum(value, -high)?;
    Some((high, low))
}

/// Checked FMA-free product transform for already split inputs. It declines
/// products whose residual arithmetic leaves its normal exponent range.
pub(crate) fn two_product_dekker_checked_parts(
    lhs: DekkerParts,
    rhs: DekkerParts,
) -> Option<(f64, f64)> {
    if lhs.is_zero() || rhs.is_zero() {
        return Some((lhs.value * rhs.value, 0.0));
    }
    let exponent_sum = lhs.exponent.checked_add(rhs.exponent)?;
    if !(DEKKER_PRODUCT_EXPONENT_MIN..=DEKKER_PRODUCT_EXPONENT_MAX).contains(&exponent_sum) {
        return None;
    }
    let product = dekker_product(lhs.value, rhs.value)?;
    let high_high = dekker_product(lhs.high, rhs.high)?;
    let low_high = dekker_product(lhs.low, rhs.high)?;
    let high_low = dekker_product(lhs.high, rhs.low)?;
    let low_low = dekker_product(lhs.low, rhs.low)?;
    let error_1 = checked_normal_sum(product, -high_high)?;
    let error_2 = checked_normal_sum(error_1, -low_high)?;
    let error_3 = checked_normal_sum(error_2, -high_low)?;
    let error = checked_normal_sum(low_low, -error_3)?;
    Some((product, error))
}

/// Error-free product for row-admitted split operands.
///
/// The row admission bounds each input exponent and their sum. Every nonzero
/// split limb is therefore an integer multiple of its input's `2^(e-52)` grid,
/// and each partial product lies on the grid `2^(e_lhs+e_rhs-104)`. Admission
/// requires that grid to be at least the subnormal quantum, so none of these
/// rounded operations can discard a nonzero residual.
pub(crate) fn two_product_dekker_admitted(lhs: DekkerParts, rhs: DekkerParts) -> (f64, f64) {
    if lhs.is_zero() || rhs.is_zero() {
        return (lhs.value * rhs.value, 0.0);
    }
    let product = lhs.value * rhs.value;
    let high_high = lhs.high * rhs.high;
    let low_high = lhs.low * rhs.high;
    let high_low = lhs.high * rhs.low;
    let low_low = lhs.low * rhs.low;
    let error_1 = product - high_high;
    let error_2 = error_1 - low_high;
    let error_3 = error_2 - high_low;
    let error = low_low - error_3;
    (product, error)
}

pub(crate) fn dd_mul(lhs: (f64, f64), rhs: (f64, f64)) -> (f64, f64) {
    let (head, head_err) = two_product(lhs.0, rhs.0);
    two_sum(head, head_err + (lhs.0 * rhs.1 + lhs.1 * rhs.0))
}

pub(crate) fn dd_add(lhs: (f64, f64), rhs: (f64, f64)) -> (f64, f64) {
    let (head, head_err) = two_sum(lhs.0, rhs.0);
    two_sum(head, head_err + (lhs.1 + rhs.1))
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "stored exponents are 1..=2046 and a nonzero subnormal significand's leading-bit index is 0..=51"
)]
pub(crate) fn floor_binary_exponent(value: f64) -> i32 {
    debug_assert!(value.is_finite() && value > 0.0);
    let bits = value.to_bits();
    let stored = ((bits >> 52) & 0x7FF) as i32;
    if stored != 0 {
        stored - 1023
    } else {
        let significand = bits & ((1_u64 << 52) - 1);
        debug_assert_ne!(significand, 0);
        let highest_bit = 63 - significand.leading_zeros() as i32;
        -1074 + highest_bit
    }
}

pub(crate) fn exact_power_of_two(shift: i16) -> f64 {
    let stored = i32::from(shift).wrapping_add(1023);
    debug_assert!((1..=2046).contains(&stored));
    f64::from_bits((stored as u64) << 52)
}

/// A product decomposition whose residual is exactly representable as binary64.
///
/// A binary64 product has at most 106 significant bits.  If its rounded head
/// has exponent `e >= -969`, its exact low bit is no smaller than `2^-1074`,
/// so the fused residual can be carried by the returned tail.  A lower normal
/// head, a subnormal head, or a nonzero product that rounds to zero may have a
/// residual below the binary64 grid and is declined rather than silently
/// returning a false expansion. Exact zero products are admitted separately.
pub(crate) fn two_product_exact(left: f64, right: f64) -> Option<(f64, f64)> {
    let product = left * right;
    if !product.is_finite() {
        return None;
    }
    if product == 0.0 {
        return (left == 0.0 || right == 0.0).then_some((product, 0.0));
    }
    if !product.is_normal() || floor_binary_exponent(product.abs()) < MIN_EXACT_PRODUCT_EXPONENT {
        return None;
    }
    let tail = left.mul_add(right, -product);
    tail.is_finite().then_some((product, tail))
}

/// [`two_product`] that declines a non-finite head instead of returning it
/// with a zero tail: the rectangle solver bails on such a product.
pub(crate) fn checked_two_product(left: f64, right: f64) -> Option<(f64, f64)> {
    let (head, tail) = two_product(left, right);
    head.is_finite().then_some((head, tail))
}

pub(crate) fn dd_sub(left: (f64, f64), right: (f64, f64)) -> (f64, f64) {
    dd_add(left, (-right.0, -right.1))
}

/// [`dd_mul`] declining a non-finite head, for the rectangle solver.
pub(crate) fn checked_dd_mul(left: (f64, f64), right: (f64, f64)) -> Option<(f64, f64)> {
    checked_two_product(left.0, right.0)?;
    let product = dd_mul(left, right);
    product.0.is_finite().then_some(product)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::{GeographicFrame, RetainedChartDisplacement, RetainedLongitudeSite};
    use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};

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
    fn scalar_and_retained_projection_share_exact_rounding_without_lift_narrowing() {
        let degree = GeographicFrame::crs84().longitude();
        let grad = GeographicFrame::try_new(
            crate::geographic::AngularFactor::try_new("longitude", std::f64::consts::PI / 200.0)
                .unwrap(),
            crate::geographic::AngularFactor::try_new("latitude", std::f64::consts::PI / 200.0)
                .unwrap(),
            0.0,
        )
        .unwrap()
        .longitude();
        let radian = GeographicFrame::try_new(
            crate::geographic::AngularFactor::try_new("longitude", 1.0).unwrap(),
            crate::geographic::AngularFactor::try_new("latitude", 1.0).unwrap(),
            0.0,
        )
        .unwrap()
        .longitude();
        let third = ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(3)).unwrap();
        let gap =
            ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 47)).unwrap();
        for (axis, exact, expected) in [
            (degree, third.clone(), 1.0 / 3.0),
            (degree, ExactRational::from_f64(-0.0), 0.0),
            (degree, ExactRational::from_f64(180.0), -180.0),
            (grad, ExactRational::from_f64(200.0), -200.0),
            (
                radian,
                ExactRational::from_f64(std::f64::consts::PI),
                -std::f64::consts::PI,
            ),
            (
                degree,
                ExactRational::from_f64(180.0).try_subtract(&gap).unwrap(),
                -180.0,
            ),
            (
                degree,
                ExactRational::from_f64(-180.0).try_subtract(&gap).unwrap(),
                -180.0,
            ),
            (degree, ExactRational::from_f64(540.0), -180.0),
            (degree, ExactRational::from_f64(-540.0), -180.0),
        ] {
            let scalar = round_native_longitude(axis, &exact).unwrap();
            let retained = retained_exact_native_site(axis, &exact).unwrap();
            let rounded = ExactNativeLongitude::round_site(axis, &retained).unwrap();
            assert_eq!(scalar.to_bits(), expected.to_bits());
            assert_eq!(rounded.native().get().to_bits(), expected.to_bits());
            assert!((-axis.half_period()..axis.half_period()).contains(&scalar));
        }
        let (borrowed, lift) = split_rational_native_site(&third, 360.0, 180.0).unwrap();
        assert!(
            matches!(borrowed, std::borrow::Cow::Borrowed(value) if std::ptr::eq(value, &third))
        );
        assert!(lift.is_zero());
        for axis in [degree, grad, radian] {
            let huge = ExactRational::from_f64(2.0_f64.powi(80))
                .try_product(&ExactRational::from_f64(axis.period()))
                .unwrap()
                .try_add(&third)
                .unwrap();
            assert_eq!(
                round_native_longitude(axis, &huge).unwrap().to_bits(),
                (1.0_f64 / 3.0).to_bits()
            );
            assert!(retained_exact_native_site(axis, &huge).is_err());
        }
        let almost_half = ExactRational::from_f64(180.0).try_subtract(&gap).unwrap();
        let overflow_site = RetainedLongitudeSite::new(
            ExactNativeLongitude::from_rational(degree, almost_half.clone()).unwrap(),
            i64::MAX,
        );
        assert!(ExactNativeLongitude::round_site(degree, &overflow_site).is_err());
        assert_eq!(
            round_native_longitude(degree, &almost_half).unwrap(),
            -180.0
        );
    }

    /// Known residuals, including magnitudes a splitter would overflow and a
    /// residual rounded to the subnormal grid.
    #[test]
    fn product_matches_known_fused_residuals() {
        let cases = [
            (
                "safe nonzero residual",
                0x3FF1_9999_9999_999A,
                0x3FF4_CCCC_CCCC_CCCD,
                0x3FF6_E147_AE14_7AE2,
                0x3C54_7AE1_47AE_1480,
            ),
            (
                "rescaled nonzero residual",
                0x7FEF_FFFF_FFFF_FFFF,
                0x3FB9_9999_9999_999A,
                0x7FB9_9999_9999_9999,
                0x7C49_9999_9999_9998,
            ),
            (
                "rescaled exact power",
                0x7E70_0000_0000_0000,
                0x0170_0000_0000_0000,
                0x3FF0_0000_0000_0000,
                0x0000_0000_0000_0000,
            ),
            (
                "underflow residual",
                0x2082_6B6C_1DD3_EF04,
                0x9FC0_6EE1_DBDA_1774,
                0x8052_EB12_A8DA_CDFD,
                0x0000_0000_0000_0002,
            ),
        ];
        for &(name, lhs_bits, rhs_bits, expected_head, expected_tail) in &cases {
            let (lhs, rhs) = (f64::from_bits(lhs_bits), f64::from_bits(rhs_bits));
            let (head, tail) = two_product(lhs, rhs);
            assert_eq!(head.to_bits(), expected_head, "{name}: head");
            assert_eq!(tail.to_bits(), expected_tail, "{name}: tail");
        }
    }

    #[test]
    fn exact_product_matches_known_fused_residuals() {
        let cases = [
            (
                "ordinary",
                0x3FF1_9999_9999_999A,
                0x3FF4_CCCC_CCCC_CCCD,
                0x3FF6_E147_AE14_7AE2,
                0x3C54_7AE1_47AE_1480,
            ),
            (
                "mixed exponent",
                0x6974_E718_D7D7_625A,
                0x01A5_6E1F_C2F8_F359,
                0x2B2B_FF2E_E48E_0530,
                0xA7B9_6B30_449B_05B0,
            ),
            (
                "maximum finite half",
                0x7FEF_FFFF_FFFF_FFFF,
                0x3FE0_0000_0000_0000,
                0x7FDF_FFFF_FFFF_FFFF,
                0x0000_0000_0000_0000,
            ),
        ];
        for &(name, lhs, rhs, expected_head, expected_tail) in &cases {
            let (lhs, rhs) = (f64::from_bits(lhs), f64::from_bits(rhs));
            let Some((head, tail)) = two_product_exact(lhs, rhs) else {
                panic!("{name}: product residual was outside the exact fast domain")
            };
            assert_eq!(head.to_bits(), expected_head, "{name}: head");
            if expected_tail == 0 {
                assert_eq!(tail, 0.0, "{name}: tail");
            } else {
                assert_eq!(tail.to_bits(), expected_tail, "{name}: tail");
            }
        }
    }

    #[test]
    fn exact_product_declines_an_unrepresentable_normal_head_residual() {
        // x² = 2^-1022 + 2^-1073 + 2^-1126. The rounded head is normal,
        // but the final term is below the binary64 subnormal grid; an FMA tail
        // cannot carry the exact residual as a single f64.
        let x = f64::from_bits(0x2000_0000_0000_0001);
        let head = x * x;
        assert!(head.is_normal());
        assert_eq!(head.to_bits(), 0x0010_0000_0000_0002);
        assert_eq!(x.mul_add(x, -head), 0.0);
        assert_eq!(two_product_exact(x, x), None);
    }

    #[test]
    fn exact_product_admits_zero_and_representable_boundaries() {
        assert_eq!(two_product_exact(0.0, f64::MAX), Some((0.0, 0.0)));
        assert_eq!(two_product_exact(-0.0, 3.0), Some((-0.0, 0.0)));

        let near_threshold = f64::from_bits(0x0360_0000_0000_0001);
        assert!(near_threshold.is_normal());
        let (_, threshold_tail) = two_product_exact(near_threshold, 1.0 + f64::EPSILON)
            .expect("the threshold residual is representable");
        assert_eq!(threshold_tail.to_bits(), 0x2);
        let (negative_head, _) = two_product_exact(-near_threshold, 1.0 + f64::EPSILON)
            .expect("negative normal product is in the exact fast domain");
        assert!(negative_head.is_sign_negative());

        // The exact product is representable, but a subnormal head is outside
        // the fast contract because its residual may require a finer grid.
        assert_eq!(two_product_exact(f64::MIN_POSITIVE, 0.5), None);
        assert_eq!(
            two_product_exact(f64::MIN_POSITIVE, f64::MIN_POSITIVE),
            None
        );
        assert!(two_product_exact(f64::MAX, 0.5).is_some());
        assert_eq!(two_product_exact(f64::MAX, 2.0), None);
        assert_eq!(two_product_exact(f64::INFINITY, 1.0), None);
        assert_eq!(two_product_exact(f64::NAN, 1.0), None);
    }

    #[test]
    fn checked_product_declines_only_a_non_finite_head() {
        assert_eq!(checked_two_product(1e200, 1e200), None);
        assert_eq!(checked_two_product(3.0, 4.0), Some((12.0, 0.0)));
        assert_eq!(
            checked_two_product(f64::MAX, 0.5).map(|(h, _)| h),
            Some(f64::from_bits(0x7FDF_FFFF_FFFF_FFFF))
        );
    }

    #[test]
    fn retained_native_sites_fold_rounded_seams_and_preserve_authentic_endpoints() {
        let axis = GeographicFrame::crs84().longitude();
        let start = 180.0_f64.next_down();
        let end = -180.0;
        let edge = axis
            .retained_native_endpoints(start, end, 1)
            .expect("the explicit producer branch is retained");

        let rounded_positive_seam = axis.native_site_at(&edge, &0.75).unwrap();
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
            axis.lifted_site_longitude(&rounded_positive_seam).unwrap(),
            180.0
        );

        let at_start = axis.native_site_at(&edge, &0.0).unwrap();
        let at_end = axis.native_site_at(&edge, &1.0).unwrap();
        assert_eq!(at_start.native().get().to_bits(), start.to_bits());
        assert_eq!(at_start.periods(), 0);
        assert_eq!(at_end.native().get().to_bits(), end.to_bits());
        assert_eq!(at_end.periods(), 1);

        let reversed = axis
            .retained_native_endpoints(end, start, -1)
            .expect("the reversed producer branch is retained");
        let rounded_negative_seam = axis.native_site_at(&reversed, &0.25).unwrap();
        assert_eq!(rounded_negative_seam.native().get(), -180.0);
        assert_eq!(rounded_negative_seam.periods(), 0);
        assert_eq!(
            axis.canonicalize(rounded_negative_seam.native().get())
                .unwrap(),
            *rounded_negative_seam.native()
        );
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
