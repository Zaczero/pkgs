//! Shared bounds for operations whose small control parameters can amplify a
//! geometry into millions of generated items.

use crate::error::Result;
use crate::geometry::GeometryErrorKind;

/// Practical ceiling for one operation's generated coordinates/cells/work
/// items. Input-sized ingestion is deliberately not charged here.
pub(crate) const GENERATED_ITEM_LIMIT: usize = 16_000_000;

/// Ceiling for the *pairwise* work a generated item count implies, when an
/// operation is quadratic in it. Two orders above [`GENERATED_ITEM_LIMIT`]
/// because a cell here is one compare rather than one stored coordinate: the
/// discrete Fréchet DP runs about 6·10⁸ cells per second, so this is roughly a
/// second and a half of work. Charged only against a caller-supplied
/// amplification parameter, never against the operand's own vertex count —
/// input size is the caller's business, `densify=1e-6` on a two-vertex line is
/// not.
pub(crate) const PAIRWISE_WORK_LIMIT: usize = 1_000_000_000;

/// Checked cumulative counter used before allocation or iterative expansion.
pub(crate) struct ExpansionBudget {
    operation: &'static str,
    parameter: &'static str,
    used: usize,
}

impl ExpansionBudget {
    pub(crate) const fn new(operation: &'static str, parameter: &'static str) -> Self {
        Self {
            operation,
            parameter,
            used: 0,
        }
    }

    /// The quadratic sibling of [`Self::product`]: `left * right` against
    /// [`PAIRWISE_WORK_LIMIT`], for an operation whose cost is the product of
    /// two generated counts rather than their sum.
    pub(crate) fn pairwise(
        operation: &'static str,
        parameter: &'static str,
        left: usize,
        right: usize,
    ) -> Result<()> {
        let produced = left
            .checked_mul(right)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if produced > PAIRWISE_WORK_LIMIT {
            return Err(GeometryErrorKind::GeneratedOutputTooLarge {
                operation,
                parameter,
                produced,
                limit: PAIRWISE_WORK_LIMIT,
            }
            .into());
        }
        Ok(())
    }

    pub(crate) fn check(
        operation: &'static str,
        parameter: &'static str,
        produced: usize,
    ) -> Result<usize> {
        if produced > GENERATED_ITEM_LIMIT {
            return Err(GeometryErrorKind::GeneratedOutputTooLarge {
                operation,
                parameter,
                produced,
                limit: GENERATED_ITEM_LIMIT,
            }
            .into());
        }
        Ok(produced)
    }

    pub(crate) fn product(
        operation: &'static str,
        parameter: &'static str,
        left: usize,
        right: usize,
    ) -> Result<usize> {
        let produced = left
            .checked_mul(right)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Self::check(operation, parameter, produced)
    }

    pub(crate) fn add(&mut self, amount: usize) -> Result<usize> {
        self.used = self.check_additional(amount)?;
        Ok(self.used)
    }

    /// Validate prospective work without committing it. Count-only emitters
    /// call this for every distinct coordinate, so a preflight cannot spend
    /// billions of iterations before discovering it exceeds the budget.
    pub(crate) fn check_additional(&self, amount: usize) -> Result<usize> {
        Self::check(
            self.operation,
            self.parameter,
            self.used
                .checked_add(amount)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
        )
    }

    pub(crate) const fn used(&self) -> usize {
        self.used
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_limit_is_allowed_and_limit_plus_one_is_rejected() {
        let mut budget = ExpansionBudget::new("test", "count");
        assert_eq!(
            budget.add(GENERATED_ITEM_LIMIT).unwrap(),
            GENERATED_ITEM_LIMIT
        );
        budget.add(1).unwrap_err();
    }

    #[test]
    fn multiplication_overflow_is_structural() {
        let error = ExpansionBudget::product("test", "count", usize::MAX, 2).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
    }

    #[test]
    fn cumulative_addition_overflow_is_structural_not_generated_work() {
        let mut budget = ExpansionBudget::new("test", "count");
        budget.add(GENERATED_ITEM_LIMIT).unwrap();
        let error = budget.check_additional(usize::MAX).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
    }

    #[test]
    fn pairwise_limit_is_independent_of_generated_output_limit() {
        assert!(ExpansionBudget::pairwise("frechet_distance", "densify", 1_000_000, 1_000).is_ok());
        assert!(
            ExpansionBudget::pairwise("frechet_distance", "densify", 1_000_000, 1_001).is_err()
        );
    }
}
