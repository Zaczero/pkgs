use std::convert::Infallible;

use super::Work;
use crate::error::{ResourceError, Result};
use crate::geometry::GeometryErrorKind;

pub(super) const BASE_EVALUATION_LIMIT: u64 = 1 << 16;

pub(super) const EVALUATIONS_PER_CONSTRUCTED_NODE: u64 = 1 << 12;

/// Two times the largest measured residual rate: ceil(2 * 548,837 / 5,472).
const EVALUATIONS_PER_PUBLISHED_LEAF: u64 = 201;

pub(super) const WORK_TICKS_PER_EVALUATION: u128 = 16;

/// One mutable owner for raw primitive work and evaluation events.
pub(in crate::geometry) trait SearchAccount: Work {
    /// Reborrow the same allowance at an explicit nested accounting boundary.
    fn budget_mut(&mut self) -> &mut EvaluationBudget;

    fn charge_evaluations(&mut self, evaluations: u64) -> Result<()> {
        self.budget_mut().debit_evaluations(evaluations)
    }
}

/// Borrowed ordinary policy: preserve evaluation events and erase primitive
/// Work until an owning Native-demand branch explicitly reborrows the budget.
pub(in crate::geometry) struct EvaluationOnly<'a> {
    budget: &'a mut EvaluationBudget,
}

impl<'a> EvaluationOnly<'a> {
    pub(in crate::geometry) const fn new(budget: &'a mut EvaluationBudget) -> Self {
        Self { budget }
    }
}

impl Work for EvaluationOnly<'_> {
    type Error = Infallible;

    fn charge(&mut self, _ticks: u64) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn charge_product(
        &mut self,
        _count: usize,
        _ticks_per_item: usize,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

impl SearchAccount for EvaluationOnly<'_> {
    fn budget_mut(&mut self) -> &mut EvaluationBudget {
        self.budget
    }
}

pub(in crate::geometry) struct EvaluationBudget {
    operation: &'static str,
    constructed: usize,
    published_leaves: usize,
    pub(super) limit: u64,
    pub(super) spent_ticks: u128,
}

impl EvaluationBudget {
    pub(in crate::geometry) fn new(operation: &'static str, constructed: usize) -> Result<Self> {
        let limit = Self::limit_for(constructed, 0)?;
        Ok(Self {
            operation,
            constructed,
            published_leaves: 0,
            limit,
            spent_ticks: 0,
        })
    }

    fn limit_for(constructed: usize, published_leaves: usize) -> Result<u64> {
        let constructed = u64::try_from(constructed).map_err(|_| ResourceError::SizeOverflow)?;
        let published_leaves =
            u64::try_from(published_leaves).map_err(|_| ResourceError::SizeOverflow)?;
        let per_node = EVALUATIONS_PER_CONSTRUCTED_NODE
            .checked_mul(constructed)
            .ok_or(ResourceError::SizeOverflow)?;
        let per_leaf = EVALUATIONS_PER_PUBLISHED_LEAF
            .checked_mul(published_leaves)
            .ok_or(ResourceError::SizeOverflow)?;
        BASE_EVALUATION_LIMIT
            .checked_add(per_node)
            .and_then(|limit| limit.checked_add(per_leaf))
            .ok_or_else(|| ResourceError::SizeOverflow.into())
    }

    #[cfg(test)]
    pub(in crate::geometry) fn with_remaining_ticks_for_test(
        operation: &'static str,
        constructed: usize,
        remaining_ticks: u128,
    ) -> Result<Self> {
        let mut budget = Self::new(operation, constructed)?;
        let limit_ticks = budget.limit_ticks();
        budget.spent_ticks = limit_ticks
            .checked_sub(remaining_ticks)
            .ok_or(ResourceError::SizeOverflow)?;
        Ok(budget)
    }

    pub(in crate::geometry) fn set_constructed(&mut self, constructed: usize) -> Result<()> {
        if constructed < self.constructed {
            return Err(GeometryErrorKind::Invalid(
                "constructed-node budget cannot decrease".into(),
            )
            .into());
        }
        let limit = Self::limit_for(constructed, self.published_leaves)?;
        self.constructed = constructed;
        self.limit = limit;
        if self.spent_ticks > self.limit_ticks() {
            return Err(self.limit_error());
        }
        Ok(())
    }

    pub(in crate::geometry) fn set_published_leaves(
        &mut self,
        published_leaves: usize,
    ) -> Result<()> {
        if published_leaves < self.published_leaves {
            return Err(
                GeometryErrorKind::Invalid("published-leaf budget cannot decrease".into()).into(),
            );
        }
        let limit = Self::limit_for(self.constructed, published_leaves)?;
        self.published_leaves = published_leaves;
        self.limit = limit;
        if self.spent_ticks > self.limit_ticks() {
            return Err(self.limit_error());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::geometry) const fn constructed(&self) -> usize {
        self.constructed
    }

    #[cfg(test)]
    pub(in crate::geometry) const fn spent_ticks_for_test(&self) -> u128 {
        self.spent_ticks
    }

    pub(in crate::geometry) fn include_constructed(&mut self, constructed: usize) -> Result<()> {
        if constructed > self.constructed {
            self.set_constructed(constructed)?;
        }
        Ok(())
    }

    /// Charge an evaluation event before running the corresponding predicate.
    fn debit_evaluations(&mut self, units: u64) -> Result<()> {
        let ticks = u128::from(units) * WORK_TICKS_PER_EVALUATION;
        self.debit_ticks(ticks)
    }

    fn debit_ticks(&mut self, ticks: u128) -> Result<()> {
        let Some(next) = self.spent_ticks.checked_add(ticks) else {
            return Err(self.limit_error());
        };
        if next > self.limit_ticks() {
            return Err(self.limit_error());
        }
        self.spent_ticks = next;
        Ok(())
    }

    /// Record work that the problem had to complete before search began.
    pub(in crate::geometry) fn record_completed_evaluations(&mut self, units: u64) -> Result<()> {
        let completed = self
            .evaluation_count()
            .checked_add(units)
            .ok_or_else(|| self.limit_error())?;
        let ticks = u128::from(units) * WORK_TICKS_PER_EVALUATION;
        self.spent_ticks = self
            .spent_ticks
            .checked_add(ticks)
            .ok_or_else(|| self.limit_error())?;
        if completed > self.limit {
            return Err(self.limit_error());
        }
        Ok(())
    }

    pub(super) fn limit_ticks(&self) -> u128 {
        u128::from(self.limit) * WORK_TICKS_PER_EVALUATION
    }

    pub(super) fn evaluation_count(&self) -> u64 {
        u64::try_from(self.spent_ticks.div_ceil(WORK_TICKS_PER_EVALUATION))
            .expect("the single raw-work balance fits the u64 evaluation error field")
    }

    fn limit_error(&self) -> crate::error::Error {
        GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: self.operation,
            constructed: self.constructed,
            evaluations: self.evaluation_count(),
        }
        .into()
    }

    pub(super) fn unrepresentable(&self) -> crate::error::Error {
        GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: self.operation,
            constructed: self.constructed,
        }
        .into()
    }
}

impl Work for EvaluationBudget {
    type Error = crate::error::Error;

    fn charge(&mut self, ticks: u64) -> std::result::Result<(), Self::Error> {
        self.debit_ticks(u128::from(ticks))
    }

    fn charge_product(
        &mut self,
        count: usize,
        ticks_per_item: usize,
    ) -> std::result::Result<(), Self::Error> {
        let ticks = (count as u128).wrapping_mul(ticks_per_item as u128);
        self.debit_ticks(ticks)
    }
}

impl SearchAccount for EvaluationBudget {
    fn budget_mut(&mut self) -> &mut EvaluationBudget {
        self
    }
}
