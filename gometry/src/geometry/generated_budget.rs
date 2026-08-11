//! Shared bounds for operations whose small control parameters can amplify a
//! geometry into millions of generated items.

use crate::error::Result;
use crate::geometry::GeometryErrorKind;

/// Practical ceiling for one operation's generated coordinates/cells/work
/// items. Input-sized ingestion is deliberately not charged here.
pub(crate) const GENERATED_ITEM_LIMIT: usize = 16_000_000;

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
        let produced = left.saturating_mul(right);
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
            self.used.saturating_add(amount),
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
    fn multiplication_overflow_is_an_ordinary_budget_error() {
        ExpansionBudget::product("test", "count", usize::MAX, 2).unwrap_err();
    }
}
