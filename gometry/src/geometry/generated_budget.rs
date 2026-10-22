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
#[derive(Clone)]
pub(crate) struct ExpansionBudget {
    operation: &'static str,
    parameter: &'static str,
    used: usize,
    limit: usize,
}

impl ExpansionBudget {
    pub(crate) const fn new(operation: &'static str, parameter: &'static str) -> Self {
        Self {
            operation,
            parameter,
            used: 0,
            limit: GENERATED_ITEM_LIMIT,
        }
    }

    /// Input-proportional publication is bounded by representable storage,
    /// rather than by the small-control-parameter amplification ceiling.
    pub(crate) const fn for_input(operation: &'static str, parameter: &'static str) -> Self {
        Self {
            operation,
            parameter,
            used: 0,
            limit: usize::MAX,
        }
    }

    pub(crate) const fn operation(&self) -> &'static str {
        self.operation
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
        let produced = left.saturating_mul(right);
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
        // A product past `usize` is past every limit.
        Self::check(operation, parameter, left.saturating_mul(right))
    }

    pub(crate) fn add(&mut self, amount: usize) -> Result<usize> {
        self.used = self.check_additional(amount)?;
        Ok(self.used)
    }

    /// Validate prospective work without committing it. Count-only emitters
    /// call this for every distinct coordinate, so a preflight cannot spend
    /// billions of iterations before discovering it exceeds the budget.
    pub(crate) fn check_additional(&self, amount: usize) -> Result<usize> {
        let produced = match self.used.checked_add(amount) {
            Some(produced) => produced,
            None if self.limit != usize::MAX => usize::MAX,
            None => return Err(crate::error::ResourceError::SizeOverflow.into()),
        };
        if produced > self.limit {
            return Err(GeometryErrorKind::GeneratedOutputTooLarge {
                operation: self.operation,
                parameter: self.parameter,
                produced,
                limit: self.limit,
            }
            .into());
        }
        Ok(produced)
    }

    #[cfg(test)]
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
    fn multiplication_overflow_saturates_into_the_budget_error() {
        let error = ExpansionBudget::product("test", "count", usize::MAX, 2).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge {
                produced: usize::MAX,
                ..
            })
        ));
    }

    #[test]
    fn cumulative_addition_overflow_saturates_into_the_budget_error() {
        let mut budget = ExpansionBudget::new("test", "count");
        budget.add(GENERATED_ITEM_LIMIT).unwrap();
        let error = budget.check_additional(usize::MAX).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge {
                produced: usize::MAX,
                ..
            })
        ));
    }

    #[test]
    fn input_proportional_publication_has_no_parameter_amplification_limit() {
        let mut budget = ExpansionBudget::for_input("line_merge", "source edges");
        assert_eq!(
            budget.add(GENERATED_ITEM_LIMIT.wrapping_add(1)).unwrap(),
            GENERATED_ITEM_LIMIT.wrapping_add(1),
        );
        assert!(matches!(
            budget.check_additional(usize::MAX).unwrap_err().kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow),
        ));
    }

    #[test]
    fn pairwise_limit_is_independent_of_generated_output_limit() {
        ExpansionBudget::pairwise("frechet_distance", "densify", 1_000_000, 1_000).unwrap();
        assert!(
            ExpansionBudget::pairwise("frechet_distance", "densify", 1_000_000, 1_001).is_err()
        );
    }
}
