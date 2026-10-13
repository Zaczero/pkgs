use crate::work::Work;

pub(super) struct RemainingWork(pub(super) u64);

impl Work for RemainingWork {
    type Error = crate::error::Error;

    fn charge(&mut self, ticks: u64) -> crate::error::Result<()> {
        let Some(remaining) = self.0.checked_sub(ticks) else {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        };
        self.0 = remaining;
        Ok(())
    }

    fn charge_product(&mut self, count: usize, ticks_per_item: usize) -> crate::error::Result<()> {
        self.charge(crate::work::work_product_ticks(count, ticks_per_item)?)
    }
}
