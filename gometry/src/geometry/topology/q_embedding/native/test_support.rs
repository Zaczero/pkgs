use super::super::*;

#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q_embedding) struct ChargeEvent {
    pub(in crate::geometry::topology::q_embedding) spent_before: u128,
    pub(in crate::geometry::topology::q_embedding) ticks: u128,
}
pub(in crate::geometry::topology::q_embedding) struct RecordingWork<'a> {
    pub(in crate::geometry::topology::q_embedding) budget: &'a mut EvaluationBudget,
    pub(in crate::geometry::topology::q_embedding) spent: u128,
    pub(in crate::geometry::topology::q_embedding) charges: Vec<ChargeEvent>,
    pub(in crate::geometry::topology::q_embedding) refused: Option<ChargeEvent>,
}
impl<'a> RecordingWork<'a> {
    pub(in crate::geometry::topology::q_embedding) fn new(
        budget: &'a mut EvaluationBudget,
    ) -> Self {
        Self {
            budget,
            spent: 0,
            charges: Vec::new(),
            refused: None,
        }
    }

    fn record(&mut self, ticks: u128, result: &Result<()>) {
        let event = ChargeEvent {
            spent_before: self.spent,
            ticks,
        };
        if result.is_ok() {
            self.charges.push(event);
            self.spent = self.spent.wrapping_add(ticks);
        } else {
            self.refused = Some(event);
        }
    }
}
impl Work for RecordingWork<'_> {
    type Error = crate::error::Error;

    fn charge(&mut self, ticks: u64) -> Result<()> {
        let result = <EvaluationBudget as Work>::charge(self.budget, ticks);
        self.record(u128::from(ticks), &result);
        result
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        let ticks = (count as u128).wrapping_mul(ticks_per_item as u128);
        let result = <EvaluationBudget as Work>::charge_product(self.budget, count, ticks_per_item);
        self.record(ticks, &result);
        result
    }
}
