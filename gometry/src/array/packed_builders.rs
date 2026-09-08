use std::sync::Arc;

use crate::array::{GeometryArrayStorage, PackedStorage, PyGeometryArray, RowProducts};

impl PyGeometryArray {
    /// Gather logical rows into a new array (packed remap or mixed
    /// materialize).
    pub(crate) fn gather_logical_rows(&self, rows: &[usize]) -> crate::error::Result<Self> {
        if self.rows.is_compact_nullable() {
            let missing = self.gather_missing(rows)?;
            let all_missing = missing
                .as_ref()
                .is_some_and(|mask| mask.present_count() == 0);
            let shapes: Vec<crate::array::Shape> = rows
                .iter()
                .filter(|&&row| !self.is_row_missing(row))
                .map(|&row| {
                    self.geometry_at(row)
                        .map(|geometry| geometry.shape.shape().clone())
                })
                .collect::<crate::error::Result<_>>()?;
            let present = if shapes.is_empty() {
                self.empty_homogeneous_like()
            } else {
                Ok(Self::from_shapes(shapes, self.frame.clone())?)
            };
            let mut result = match missing {
                None => present?,
                Some(mask) => Self::scatter_present_rows(&present?, mask)?,
            };
            result.row_products = if all_missing {
                RowProducts::fresh(rows.len())
            } else {
                self.selected_row_products(rows.iter().copied())
            };
            return Ok(result);
        }
        Ok(self
            .gather_logical_rows_dense(rows)?
            .with_missing_mask(self.gather_missing(rows)?)?
            .with_selected_caches_from(self, rows.iter().copied()))
    }

    /// Gather a contiguous logical row range without allocating an index Vec.
    pub(crate) fn gather_logical_row_range(
        &self,
        rows: std::ops::Range<usize>,
    ) -> crate::error::Result<Self> {
        if self.rows.is_compact_nullable() {
            let mask = self.gather_missing_range(rows.clone());
            let products = if mask.as_ref().is_some_and(|mask| mask.present_count() == 0) {
                RowProducts::fresh(rows.len())
            } else {
                self.selected_row_products(rows.clone())
            };
            let selected = match self.storage() {
                GeometryArrayStorage::Packed(PackedStorage::Lines {
                    coords,
                    offsets,
                    row_map,
                }) => self.remap_packed_lines(Arc::clone(coords), offsets.clone(), row_map, rows),
                GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords,
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                }) => self.remap_packed_polygons(
                    Arc::clone(coords),
                    ring_offsets.clone(),
                    polygon_offsets.clone(),
                    row_map,
                    rows,
                ),
                GeometryArrayStorage::Packed(PackedStorage::Points { .. })
                | GeometryArrayStorage::Mixed(_) => {
                    return self.gather_logical_rows(&rows.collect::<Vec<_>>());
                },
            };
            let Some(mask) = mask else {
                let mut result = selected;
                result.row_products = products;
                return Ok(result);
            };
            let mut result =
                Self::compact_from_storage(selected.storage().clone(), mask, self.frame.clone());
            result.row_products = products;
            return Ok(result);
        }
        Ok(self
            .gather_logical_row_range_dense(rows.clone())?
            .with_missing_mask(self.gather_missing_range(rows.clone()))?
            .with_selected_caches_from(self, rows))
    }

    pub(crate) fn gather_logical_rows_dense(&self, rows: &[usize]) -> crate::error::Result<Self> {
        if self.rows.is_compact_nullable() {
            let shapes = rows
                .iter()
                .map(|&row| {
                    self.geometry_at(row)
                        .map(|geometry| geometry.shape.shape().clone())
                })
                .collect::<crate::error::Result<Vec<_>>>()?;
            return Ok(Self::from_shapes(shapes, self.frame.clone())?);
        }
        if let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
            self.storage()
        {
            return Ok(self.select_packed_points(
                Arc::clone(coords),
                row_map,
                rows.iter().copied(),
            ));
        }
        if let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        }) = self.storage()
        {
            return Ok(self.remap_packed_lines(
                Arc::clone(coords),
                offsets.clone(),
                row_map,
                rows.iter().copied(),
            ));
        }
        if let GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        }) = self.storage()
        {
            return Ok(self.remap_packed_polygons(
                Arc::clone(coords),
                ring_offsets.clone(),
                polygon_offsets.clone(),
                row_map,
                rows.iter().copied(),
            ));
        }
        let shapes = match self.storage() {
            GeometryArrayStorage::Mixed(existing) => rows
                .iter()
                .map(|&row| Ok(existing[row].clone()))
                .collect::<crate::error::Result<Vec<_>>>()?,
            GeometryArrayStorage::Packed(_) => rows
                .iter()
                .map(|&row| self.storage().row(row).with_shape(std::clone::Clone::clone))
                .collect::<crate::error::Result<Vec<_>>>()?,
        };
        Ok(Self::mixed_shapes(shapes, self.frame.clone()))
    }

    fn gather_logical_row_range_dense(
        &self,
        rows: std::ops::Range<usize>,
    ) -> crate::error::Result<Self> {
        if let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
            self.storage()
        {
            return Ok(self.select_packed_points(Arc::clone(coords), row_map, rows));
        }
        if let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        }) = self.storage()
        {
            return Ok(self.remap_packed_lines(Arc::clone(coords), offsets.clone(), row_map, rows));
        }
        if let GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        }) = self.storage()
        {
            return Ok(self.remap_packed_polygons(
                Arc::clone(coords),
                ring_offsets.clone(),
                polygon_offsets.clone(),
                row_map,
                rows,
            ));
        }
        let shapes = match self.storage() {
            GeometryArrayStorage::Mixed(existing) => existing[rows].to_vec(),
            GeometryArrayStorage::Packed(_) => rows
                .map(|row| self.storage().row(row).with_shape(std::clone::Clone::clone))
                .collect::<crate::error::Result<Vec<_>>>()?,
        };
        Ok(Self::mixed_shapes(shapes, self.frame.clone()))
    }
}
