use std::sync::Arc;

use crate::array::{
    GeometryArrayStorage, PackedStorage, PointColumnBuilder, PyGeometryArray, RowSelection,
    StorageLayout, ValidityMask, row_selection_from_logical_rows,
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
        if self.is_compact_nullable()
            && let Some(mask) = self.missing()
            && matches!(
                self.storage(),
                GeometryArrayStorage::Packed(PackedStorage::Points { .. })
            )
        {
            let present = mask.present_before(rows.start)..mask.present_before(rows.end);
            let storage = self.selected_point_present_range(present)?;
            return Self::from_selected_storage(
                self,
                Arc::new(storage),
                self.frame.clone(),
                missing,
                StorageLayout::PresentOnly,
                rows,
            );
        }
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
            Arc::new(storage),
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
            Arc::new(self.selected_storage(rows.iter().copied())?),
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
                let present = if let Some(mask) = missing {
                    let logical_rows = rows.collect::<Vec<_>>();
                    let rank = mask.rank_for_rows(&logical_rows)?;
                    logical_rows
                        .into_iter()
                        .filter_map(|row| rank.resolve(row))
                        .collect::<Vec<_>>()
                } else {
                    rows.collect::<Vec<_>>()
                };
                if present.is_empty() {
                    GeometryArrayStorage::Packed(PackedStorage::Points {
                        coords: Arc::new(
                            crate::array::PointColumnBuilder::try_with_capacity(coords.axes(), 0)?
                                .finish()?,
                        ),
                        row_map: RowSelection::Identity,
                    })
                } else if let Some(window) =
                    contiguous_physical_range(row_map.as_deref(), present.iter().copied())
                {
                    GeometryArrayStorage::Packed(PackedStorage::Points {
                        coords: Arc::new(coords.view(crate::geometry::CoordWindow::from_range(
                            window,
                            coords.len(),
                        ))),
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

    fn selected_point_present_range(
        &self,
        present: std::ops::Range<usize>,
    ) -> crate::error::Result<GeometryArrayStorage> {
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
            self.storage()
        else {
            unreachable!("compact point range selection requires packed points");
        };
        if present.is_empty() {
            return Ok(GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(
                    PointColumnBuilder::try_with_capacity(coords.axes(), 0)?.finish()?,
                ),
                row_map: RowSelection::Identity,
            }));
        }
        if let Some(window) =
            crate::array::contiguous_physical_range(row_map.as_deref(), present.clone())
        {
            return Ok(GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: Arc::new(coords.view(crate::geometry::CoordWindow::from_range(
                    window,
                    coords.len(),
                ))),
                row_map: RowSelection::Identity,
            }));
        }
        Ok(GeometryArrayStorage::Packed(PackedStorage::Points {
            coords: Arc::clone(coords),
            row_map: row_selection_from_logical_rows(row_map.as_deref(), coords.len(), present)?,
        }))
    }
}
