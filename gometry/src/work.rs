//! Shared raw-work accounting policies.

use std::convert::Infallible;

#[cfg(test)]
use crate::error::ResourceError;
use crate::error::Result;

/// Raw primitive work. Evaluation events remain a separate unit on the same
/// account; one evaluation consumes sixteen raw work ticks.
pub(crate) trait Work {
    /// The policy's failure type; ordinary policies use `Infallible`.
    type Error: Into<crate::error::Error>;

    fn charge(&mut self, ticks: u64) -> std::result::Result<(), Self::Error>;

    fn charge_product(
        &mut self,
        count: usize,
        ticks_per_item: usize,
    ) -> std::result::Result<(), Self::Error>;

    fn charge_into_error(&mut self, ticks: u64) -> Result<()> {
        self.charge(ticks).map_err(Into::into)
    }

    fn charge_product_into_error(&mut self, count: usize, ticks_per_item: usize) -> Result<()> {
        self.charge_product(count, ticks_per_item)
            .map_err(Into::into)
    }
}

pub(crate) type WorkResult<W, T> = std::result::Result<T, <W as Work>::Error>;

#[cfg(test)]
pub(crate) fn work_product_ticks(count: usize, ticks_per_item: usize) -> Result<u64> {
    let ticks = count
        .checked_mul(ticks_per_item)
        .ok_or(ResourceError::SizeOverflow)?;
    u64::try_from(ticks).map_err(|_| ResourceError::SizeOverflow.into())
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Unmetered;

impl Work for Unmetered {
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
