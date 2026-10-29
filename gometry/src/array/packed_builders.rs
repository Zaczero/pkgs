use std::sync::Arc;

use crate::array::PyGeometryArray;
use crate::geometry::storage::gather::materialized_storage;
use crate::geometry::storage::{
    GeometryArrayStorage, PackedStorage, StorageLayout, ValidityBuilder,
};

impl PyGeometryArray {
    pub(crate) fn materialize_rows(&self) -> crate::error::Result<Self> {
        let storage = materialized_storage(self.storage())?;
        let missing = self.missing().and_then(|mask| {
            let mut validity = ValidityBuilder::with_capacity(self.len());
            for index in 0..self.len() {
                validity.push(mask.is_valid(index));
            }
            validity.finish()
        });
        let layout = if matches!(
            &storage,
            GeometryArrayStorage::Packed(PackedStorage::Points { .. })
        ) && missing.is_some()
        {
            StorageLayout::PresentOnly
        } else {
            StorageLayout::LogicalRows
        };
        Self::from_result_storage(
            Arc::new(storage),
            self.frame.clone(),
            missing,
            layout,
            crate::array::ComputedCaches::fresh(),
        )
    }

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
