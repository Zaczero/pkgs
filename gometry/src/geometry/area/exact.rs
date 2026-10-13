use super::*;

/// Exact rational accumulator for areas of selected partition faces.
///
/// A constructed partition node is a rational point, not a binary64 point:
/// its homogeneous coordinates are dyadic numerators and denominators. The
/// selected-face cross products therefore form a rational sum. Keep that sum
/// reduced after every term, then round its final absolute half-area once with
/// the exact ratio owner. This is deliberately separate from [`ExactSum`],
/// whose degree-two dyadic contract serves ring winding signs and source-ring
/// magnitudes rather than rational face coordinates.
pub(crate) struct ExactRationalArea {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
}

impl ExactRationalArea {
    pub(in crate::geometry) fn new() -> Self {
        Self {
            numerator: ExactDyadic::from_f64(0.0),
            denominator: ExactDyadic::from_f64(1.0),
        }
    }

    pub(in crate::geometry) fn add_fraction(
        &mut self,
        mut numerator: ExactDyadic,
        mut denominator: ExactDyadic,
    ) -> Result<()> {
        if numerator.is_zero() {
            return Ok(());
        }
        if denominator.is_zero() {
            return Err(crate::geometry::GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        if denominator.sign() == ExactSign::Negative {
            numerator = numerator.neg();
            denominator = denominator.neg();
        }

        let common = self.denominator.gcd(&denominator);
        let left_scale = self.denominator.try_exact_quotient(&common)?;
        let right_scale = denominator.try_exact_quotient(&common)?;
        let left = self.numerator.try_product(&right_scale)?;
        let right = numerator.try_product(&left_scale)?;
        let next_numerator = left.try_add(right)?;
        let next_denominator = self.denominator.try_product(&right_scale)?;

        // Keep the common denominator bounded by the least common multiple,
        // rather than the product of every face denominator. This is the
        // rational-sum contract: every update remains an exact reduced
        // fraction, and only the final quotient is rounded.
        let reduction = next_numerator.clone().abs().gcd(&next_denominator);
        self.numerator = next_numerator.try_exact_quotient(&reduction)?;
        self.denominator = next_denominator.try_exact_quotient(&reduction)?;
        Ok(())
    }

    pub(in crate::geometry) fn add_homogeneous_cross(
        &mut self,
        left: &[ExactDyadic; 3],
        right: &[ExactDyadic; 3],
    ) -> Result<()> {
        let forward = left[0].try_product(&right[1])?;
        let reverse = left[1].try_product(&right[0])?;
        let numerator = forward.try_subtract(reverse)?;
        let denominator = left[2].try_product(&right[2])?;
        self.add_fraction(numerator, denominator)
    }

    pub(in crate::geometry) fn round_abs_half(&self) -> Result<f64> {
        let numerator = self.numerator.clone().abs();
        let denominator = self.denominator.try_product(&ExactDyadic::from_f64(2.0))?;
        match round_ratio(&numerator, &denominator) {
            Ok(value) => Ok(value),
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        crate::geometry::GeometryErrorKind::ExactCoordinateOutOfRange
                    )
                ) =>
            {
                // A scalar area uses IEEE binary64 overflow semantics. The
                // shared ratio owner reports the same threshold as an
                // unrepresentable coordinate, so translate only that
                // magnitude-specific result to positive infinity.
                Ok(f64::INFINITY)
            },
            Err(error) => Err(error),
        }
    }

    pub(in crate::geometry) const fn sign(&self) -> ExactSign {
        self.numerator.sign()
    }
}

/// One polynomial integral with a shared factorial denominator.
///
/// Geographic density moments use `1 / (degree + 2)!` after Green integration.
/// Keeping one numerator avoids reducing a generic rational after every degree.
pub(crate) struct ExactFactorialArea {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
}

impl ExactFactorialArea {
    pub(in crate::geometry) fn dyadic_within_gamma(
        error: &ExactDyadic,
        norm: &ExactDyadic,
        operation_count: u64,
    ) -> Result<bool> {
        let operation_count_i64 = i64::try_from(operation_count)
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        let denominator = (1_i64 << 53)
            .checked_sub(operation_count_i64)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let left = error.try_product(&ExactDyadic::from_i64(denominator))?;
        let right = norm.try_product(&ExactDyadic::from_i64(operation_count_i64))?;
        Ok(left.cmp(&right).is_le())
    }

    pub(in crate::geometry) fn from_derivatives(
        derivatives: &[ExactDyadic],
        moments: &[ExactDyadic],
    ) -> Result<Self> {
        Self::from_products(
            derivatives
                .iter()
                .zip(moments)
                .map(|(derivative, moment)| move || derivative.try_product(moment)),
        )
    }

    pub(in crate::geometry) fn absolute_contraction(
        derivatives: &[ExactDyadic],
        moments: &[ExactDyadic],
    ) -> Result<Self> {
        Self::from_products(derivatives.iter().zip(moments).map(|(derivative, moment)| {
            move || derivative.try_product(moment).map(ExactDyadic::abs)
        }))
    }

    pub(super) fn from_products(
        products: impl Iterator<Item = impl FnOnce() -> Result<ExactDyadic>>,
    ) -> Result<Self> {
        let mut numerator = ExactDyadic::zero();
        let mut denominator = ExactDyadic::one();
        for (degree, product) in products.enumerate() {
            if degree == 0 {
                denominator = ExactDyadic::from_i64(2);
            } else {
                // Both callers zip non-ZST ExactDyadic slices, so their
                // representable lengths bound degree and prevent wrapping.
                let factor = i64::try_from(degree.wrapping_add(2))
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                let factor = ExactDyadic::from_i64(factor);
                numerator = numerator.try_product(&factor)?;
                denominator = denominator.try_product(&factor)?;
            }
            numerator = numerator.try_add(product()?)?;
        }
        Ok(Self {
            numerator,
            denominator,
        })
    }

    pub(in crate::geometry) const fn sign(&self) -> ExactSign {
        self.numerator.sign()
    }

    pub(in crate::geometry) fn compare_dyadic_gamma(
        &self,
        error: &ExactDyadic,
        operation_count: u64,
    ) -> Result<std::cmp::Ordering> {
        let significand = 1_i64 << 53;
        let denominator = significand
            .checked_sub(
                i64::try_from(operation_count)
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            )
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let left = error
            .try_product(&ExactDyadic::from_i64(denominator))?
            .try_product(&self.denominator)?;
        let right = self.numerator.try_product(&ExactDyadic::from_i64(
            i64::try_from(operation_count)
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        ))?;
        Ok(left.cmp(&right))
    }

    pub(in crate::geometry) fn scaled_round(&self, scale: &ExactDyadic) -> Result<f64> {
        let numerator = self.numerator.try_product(scale)?;
        match round_ratio(&numerator, &self.denominator) {
            Ok(value) => Ok(value),
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        crate::geometry::GeometryErrorKind::ExactCoordinateOutOfRange
                    )
                ) =>
            {
                Ok(if self.sign() == ExactSign::Negative {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                })
            },
            Err(error) => Err(error),
        }
    }
}
