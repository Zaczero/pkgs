use std::sync::Arc;

use crate::array::{
    GeometryArrayStorage, PackedStorage, PyGeometryArray, RowSelection, StorageLayout, ValidityMask,
};

impl PyGeometryArray {
    pub(crate) fn gather_logical_rows(&self, rows: &[usize]) -> crate::error::Result<Self> {
        self.select_logical_rows(rows.iter().copied(), self.gather_missing(rows)?)
    }

    /// Gather a contiguous logical row range without allocating an index vector.
    pub(crate) fn gather_logical_row_range(
        &self,
        rows: std::ops::Range<usize>,
    ) -> crate::error::Result<Self> {
        let missing = self.gather_missing_range(rows.clone());
        self.select_logical_rows(rows, missing)
    }

    fn select_logical_rows(
        &self,
        rows: impl Iterator<Item = usize> + Clone,
        missing: Option<ValidityMask>,
    ) -> crate::error::Result<Self> {
        let storage = self.selected_storage(rows.clone())?;
        let layout = if self.is_compact_nullable()
            && matches!(
                &storage,
                GeometryArrayStorage::Packed(PackedStorage::Points { .. })
            ) {
            StorageLayout::PresentOnly
        } else {
            StorageLayout::LogicalRows
        };
        Self::from_selected_storage(
            self,
            Arc::try_new(storage).map_err(crate::error::Error::from)?,
            self.frame.clone(),
            missing,
            layout,
            rows,
        )
    }

    /// Select present rows without attaching a validity mask.
    pub(crate) fn gather_logical_rows_dense(&self, rows: &[usize]) -> crate::error::Result<Self> {
        debug_assert!(
            !self.is_compact_nullable() || rows.iter().all(|&row| !self.is_row_missing(row))
        );
        Self::from_selected_storage(
            self,
            Arc::try_new(self.selected_storage(rows.iter().copied())?)
                .map_err(crate::error::Error::from)?,
            self.frame.clone(),
            None,
            StorageLayout::LogicalRows,
            rows.iter().copied(),
        )
    }

    pub(crate) fn selected_storage(
        &self,
        rows: impl Iterator<Item = usize> + Clone,
    ) -> crate::error::Result<GeometryArrayStorage> {
        use crate::array::{contiguous_physical_range, row_selection_from_logical_rows};
        Ok(match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                let missing = self.missing().filter(|_| self.is_compact_nullable());
                let present = rows.filter_map(|row| match missing {
                    Some(mask) if mask.is_missing(row) => None,
                    Some(mask) => Some(mask.present_before(row)),
                    None => Some(row),
                });
                if present.clone().next().is_none() {
                    GeometryArrayStorage::Packed(PackedStorage::Points {
                        coords: Arc::try_new(
                            crate::array::PointColumnBuilder::try_with_capacity(coords.axes(), 0)?
                                .finish()?,
                        )
                        .map_err(crate::error::Error::from)?,
                        row_map: RowSelection::Identity,
                    })
                } else if let Some(window) =
                    contiguous_physical_range(row_map.as_deref(), present.clone())
                {
                    GeometryArrayStorage::Packed(PackedStorage::Points {
                        coords: Arc::try_new(coords.view(
                            crate::geometry::CoordWindow::from_range(window, coords.len()),
                        ))
                        .map_err(crate::error::Error::from)?,
                        row_map: RowSelection::Identity,
                    })
                } else {
                    GeometryArrayStorage::Packed(PackedStorage::Points {
                        coords: Arc::clone(coords),
                        row_map: row_selection_from_logical_rows(
                            row_map.as_deref(),
                            coords.len(),
                            present,
                        )?,
                    })
                }
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: Arc::clone(coords),
                offsets: offsets.clone(),
                row_map: row_selection_from_logical_rows(
                    row_map.as_deref(),
                    offsets.len() - 1,
                    rows,
                )?,
            }),
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: Arc::clone(coords),
                ring_offsets: ring_offsets.clone(),
                polygon_offsets: polygon_offsets.clone(),
                row_map: row_selection_from_logical_rows(
                    row_map.as_deref(),
                    polygon_offsets.len() - 1,
                    rows,
                )?,
            }),
            GeometryArrayStorage::Mixed(shapes) => {
                let mut selected = Vec::new();
                for row in rows {
                    crate::try_push_vec(&mut selected, shapes[row].clone())?;
                }
                GeometryArrayStorage::Mixed(selected)
            },
        })
    }
}
