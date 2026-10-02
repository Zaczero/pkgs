//! Ragged output construction with checked, monotonic row offsets.

use crate::error::{ResourceError, Result};

pub(crate) trait CsrOffset: Copy + Default + Ord {
    fn checked_end(end: usize) -> Result<Self>;
}

impl CsrOffset for i64 {
    fn checked_end(end: usize) -> Result<Self> {
        Self::try_from(end).map_err(|_| ResourceError::SizeOverflow.into())
    }
}

impl CsrOffset for i32 {
    fn checked_end(end: usize) -> Result<Self> {
        Self::try_from(end)
            .map_err(|_| crate::geometry::GeometryErrorKind::OffsetCapacityExceeded.into())
    }
}

/// One generic CSR owner for ragged output. Every row end is monotonic and
/// representable in its stored offset width before it is retained. Empty offsets
/// mean the implicit initial zero; the finished zero-row representation is `[0]`.
pub(crate) struct CsrBuilder<T, Offset = i64> {
    values: Vec<T>,
    offsets: Vec<Offset>,
}

impl<T, Offset: CsrOffset> CsrBuilder<T, Offset> {
    pub(crate) const fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Reserve exact, already-validated capacities for values and rows.
    pub(crate) fn with_capacity(value_capacity: usize, row_capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(value_capacity),
            offsets: Vec::with_capacity(row_capacity.wrapping_add(1)),
        }
    }

    pub(crate) fn reserve_rows(&mut self, additional: usize) {
        self.offsets
            .reserve(additional.wrapping_add(usize::from(self.offsets.is_empty())));
    }

    fn release(&mut self) {
        self.values = Vec::new();
        self.offsets = Vec::new();
    }

    pub(crate) fn push_row(&mut self, values: impl IntoIterator<Item = T>) -> Result<()> {
        self.values.extend(values);
        let end = self.values.len();
        self.push_end(end)
    }

    /// Append a checked row end after values have been emitted. This is also
    /// used by packed representations whose physical value count differs from
    /// the logical row count (for example, four vertices per triangle).
    pub(crate) fn push_end(&mut self, end: usize) -> Result<()> {
        let end = match Offset::checked_end(end) {
            Ok(end) => end,
            Err(error) => {
                self.release();
                return Err(error);
            },
        };
        let last = self.offsets.last().copied().unwrap_or_default();
        if end < last {
            self.release();
            return Err(crate::geometry::GeometryErrorKind::MalformedCsrOffsets.into());
        }
        if self.offsets.is_empty() {
            self.offsets.push(Offset::default());
        }
        self.offsets.push(end);
        Ok(())
    }

    pub(crate) const fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) const fn row_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub(crate) const fn values_mut(&mut self) -> &mut Vec<T> {
        &mut self.values
    }

    pub(crate) fn finish(self) -> (Vec<T>, Vec<Offset>) {
        let Self {
            values,
            mut offsets,
        } = self;
        if offsets.is_empty() {
            offsets.push(Offset::default());
        }
        (values, offsets)
    }
}

#[cfg(test)]
impl<T> CsrBuilder<T> {
    pub(crate) fn row_range(&self, row: usize) -> std::ops::Range<usize> {
        let next_row = row
            .checked_add(1)
            .expect("a CSR row index has a following offset");
        let start = self
            .offsets
            .get(row)
            .expect("row index belongs to the CSR builder");
        let end = self
            .offsets
            .get(next_row)
            .expect("every CSR row has a terminal offset");
        *start as usize..*end as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csr_builder_preserves_empty_rows_and_checked_offsets() {
        let mut builder = CsrBuilder::<u8>::with_capacity(4, 3);
        builder.push_row(std::iter::empty()).expect("empty row");
        builder.push_row([1, 2]).expect("values");
        builder
            .push_row(std::iter::empty())
            .expect("second empty row");
        assert_eq!(builder.row_range(1), 0..2);
        let (values, offsets) = builder.finish();
        assert_eq!(values, [1, 2]);
        assert_eq!(offsets, [0, 0, 2, 2]);
    }

    #[test]
    fn csr_builder_rejects_non_monotonic_and_non_i64_offsets() {
        let mut builder = CsrBuilder::<u8>::new();
        builder.push_end(3).expect("first end");
        assert!(builder.push_end(2).is_err());
        builder.push_row([2]).expect("reuse after invalid row end");
        let (values, offsets) = builder.finish();
        assert_eq!(values, [2]);
        assert_eq!(offsets, [0, 1]);

        let mut builder = CsrBuilder::<u8>::new();
        if usize::BITS > 63 {
            assert!(builder.push_end(i64::MAX as usize + 1).is_err());
            let (values, offsets) = builder.finish();
            assert!(values.is_empty());
            assert_eq!(offsets, [0]);
        }
    }
}
