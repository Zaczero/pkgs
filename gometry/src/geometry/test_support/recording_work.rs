use crate::geometry::embedding_search::account::EvaluationBudget;
use crate::work::Work;

pub(crate) struct RecordingWork {
    budget: EvaluationBudget,
    pub(crate) attempted: Vec<u64>,
    pub(crate) accepted: Vec<u64>,
}

impl RecordingWork {
    pub(crate) fn with_remaining(
        operation: &'static str,
        remaining_ticks: u128,
    ) -> crate::error::Result<Self> {
        Ok(Self {
            budget: EvaluationBudget::with_remaining_ticks_for_test(operation, 1, remaining_ticks)?,
            attempted: Vec::new(),
            accepted: Vec::new(),
        })
    }
}

impl Work for RecordingWork {
    type Error = crate::error::Error;

    fn charge(&mut self, ticks: u64) -> crate::error::Result<()> {
        self.attempted.push(ticks);
        self.budget.charge(ticks)?;
        self.accepted.push(ticks);
        Ok(())
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> crate::error::Result<()> {
        self.charge(crate::work::work_product_ticks(count, ticks_per_item)?)
    }
}
