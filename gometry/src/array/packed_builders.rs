use std::sync::Arc;

use super::*;

impl PyGeometryArray {
    /// Gather logical rows into a new array (packed remap or mixed
    /// materialize).
    pub(crate) fn gather_logical_rows(&self, rows: &[usize]) -> Self {
        self.gather_logical_rows_dense(rows)
            .with_missing_mask(self.gather_missing(rows))
            .with_selected_caches_from(self, rows.iter().copied())
    }

    /// Gather a contiguous logical row range without allocating an index Vec.
    pub(crate) fn gather_logical_row_range(&self, rows: std::ops::Range<usize>) -> Self {
        self.gather_logical_row_range_dense(rows.clone())
            .with_missing_mask(self.gather_missing_range(rows.clone()))
            .with_selected_caches_from(self, rows)
    }

    pub(crate) fn gather_logical_rows_dense(&self, rows: &[usize]) -> Self {
        if let GeometryArrayStorage::Points { coords, row_map } = self.storage() {
            return self.select_packed_points(Arc::clone(coords), row_map, rows.iter().copied());
        }
        if let GeometryArrayStorage::Lines {
            coords,
            offsets,
            row_map,
        } = self.storage()
        {
            return self.remap_packed_lines(
                Arc::clone(coords),
                offsets.clone(),
                row_map,
                rows.iter().copied(),
            );
        }
        if let GeometryArrayStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } = self.storage()
        {
            return self.remap_packed_polygons(
                Arc::clone(coords),
                ring_offsets.clone(),
                polygon_offsets.clone(),
                row_map,
                rows.iter().copied(),
            );
        }
        let items = rows
            .iter()
            .copied()
            .map(|row| {
                self.storage()
                    .geometry_at(row, self.frame.clone(), self.row_frame_cache(row))
            })
            .collect();
        Self::mixed(items, self.frame.clone())
    }

    fn gather_logical_row_range_dense(&self, rows: std::ops::Range<usize>) -> Self {
        if let GeometryArrayStorage::Points { coords, row_map } = self.storage() {
            return self.select_packed_points(Arc::clone(coords), row_map, rows);
        }
        if let GeometryArrayStorage::Lines {
            coords,
            offsets,
            row_map,
        } = self.storage()
        {
            return self.remap_packed_lines(Arc::clone(coords), offsets.clone(), row_map, rows);
        }
        if let GeometryArrayStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } = self.storage()
        {
            return self.remap_packed_polygons(
                Arc::clone(coords),
                ring_offsets.clone(),
                polygon_offsets.clone(),
                row_map,
                rows,
            );
        }
        let items = rows
            .map(|row| {
                self.storage()
                    .geometry_at(row, self.frame.clone(), self.row_frame_cache(row))
            })
            .collect();
        Self::mixed(items, self.frame.clone())
    }
}
