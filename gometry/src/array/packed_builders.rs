use crate::array::PyGeometryArray;

impl PyGeometryArray {
    pub(crate) fn gather_logical_rows(&self, rows: &[usize]) -> crate::error::Result<Self> {
        self.gather_logical_rows_iter(rows.iter().copied())
    }

    pub(crate) fn gather_logical_rows_iter(
        &self,
        source_rows: impl ExactSizeIterator<Item = usize> + Clone,
    ) -> crate::error::Result<Self> {
        let rows = self.rows.select(source_rows.clone())?;
        Ok(Self::from_selected_rows(
            self,
            rows,
            self.frame.clone(),
            source_rows,
        ))
    }

    /// Gather a contiguous logical range without allocating an index vector.
    pub(crate) fn gather_logical_row_range(
        &self,
        source_rows: std::ops::Range<usize>,
    ) -> crate::error::Result<Self> {
        let rows = self.rows.select_range(source_rows.clone())?;
        Ok(Self::from_selected_rows(
            self,
            rows,
            self.frame.clone(),
            source_rows,
        ))
    }
}
