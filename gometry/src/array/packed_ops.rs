#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use crate::array::{
    ArrayRow, Bound, CoordSeq, CsrOffsetBuilder, CsrOffsetColumn, GeometryArrayStorage,
    PackedColumnBuilder, PackedStorage, Point, PointColumnBuilder, PolygonLevel, PyAny, PyGeometry,
    PyGeometryArray, PyResult, Python, Result, RingLevel, RowSelection, RowSelectionRef, Shape,
    column_window, concat_coord_columns, curves, line_logical_len, note_array_row,
    packed_lines_coord_len, packed_polygons_coord_len, packed_polygons_ring_len,
    parse_curve_bounds, physical_row, point_logical_len, polygon_logical_len,
    row_selection_from_logical_rows, rows_err,
};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct LogicalRow(usize);

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct PhysicalRow(usize);

fn resolve_physical_row(row_map: RowSelectionRef<'_>, logical: LogicalRow) -> PhysicalRow {
    PhysicalRow(physical_row(row_map, logical.0))
}

fn resolve_physical_rows(
    len: usize,
    row_map: RowSelectionRef<'_>,
) -> impl ExactSizeIterator<Item = PhysicalRow> + '_ {
    (0..len).map(move |logical| resolve_physical_row(row_map, LogicalRow(logical)))
}

fn row_selection_from_typed_logical_rows(
    row_map: RowSelectionRef<'_>,
    physical_len: usize,
    rows: impl IntoIterator<Item = LogicalRow>,
) -> RowSelection {
    row_selection_from_logical_rows(row_map, physical_len, rows.into_iter().map(|row| row.0))
}

fn selected_row_map_heap_bytes(
    row_map: RowSelectionRef<'_>,
    rows: std::ops::Range<usize>,
) -> usize {
    let Some(first_logical) = rows.clone().next() else {
        return 0;
    };
    let first_physical = physical_row(row_map, first_logical);
    if rows.clone().enumerate().all(|(offset, logical)| {
        first_physical.checked_add(offset) == Some(physical_row(row_map, logical))
    }) {
        0
    } else {
        rows.len() * std::mem::size_of::<usize>()
    }
}

impl PyGeometryArray {
    fn concat_packed_points_many(
        arrays: &[&Self],
        first: &Self,
        first_coords: &CoordSeq,
    ) -> PyResult<Option<Self>> {
        let total_rows = arrays.iter().try_fold(0_usize, |total, array| {
            let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
                array.storage()
            else {
                return Ok::<_, pyo3::PyErr>(total);
            };
            total
                .checked_add(point_logical_len(coords, row_map.as_deref()))
                .ok_or_else(|| {
                    pyo3::exceptions::PyOverflowError::new_err("row count overflows usize")
                })
        })?;
        let mut builder = PointColumnBuilder::try_with_capacity(first_coords.axes(), total_rows)?;
        for array in arrays {
            let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
                array.storage()
            else {
                return Ok(None);
            };
            if coords.axes() != first_coords.axes() {
                return Ok(None);
            }
            let map = row_map.as_deref();
            for logical in 0..point_logical_len(coords, map) {
                builder.push_at(coords, physical_row(map, logical))?;
            }
        }
        Ok(Some(Self::packed_points(
            builder.finish()?,
            first.frame.clone(),
        )))
    }

    fn concat_packed_lines_many(
        arrays: &[&Self],
        first: &Self,
        first_coords: &CoordSeq,
    ) -> PyResult<Option<Self>> {
        let mut vertex_count = 0_usize;
        for array in arrays {
            let GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) = array.storage()
            else {
                return Ok(None);
            };
            if coords.axes() != first_coords.axes() {
                return Ok(None);
            }
            vertex_count = vertex_count
                .checked_add(packed_lines_coord_len(offsets, row_map.as_deref()))
                .ok_or_else(|| {
                    pyo3::exceptions::PyOverflowError::new_err("coordinate count overflows usize")
                })?;
        }
        let mut builder = PointColumnBuilder::try_with_capacity(first_coords.axes(), vertex_count)?;
        let mut offset_builder = CsrOffsetBuilder::new();
        for array in arrays {
            let GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) = array.storage()
            else {
                unreachable!("storage kind checked in reservation pass");
            };
            let map = row_map.as_deref();
            for logical in 0..array.len() {
                builder.extend_window(coords, map.csr_window(offsets, logical))?;
                offset_builder.push_end(builder.len(), vertex_count)?;
            }
        }
        let coords = builder.finish()?;
        let offsets = offset_builder.finish(vertex_count)?;
        Ok(Some(Self::packed_lines(
            coords,
            offsets,
            first.frame.clone(),
        )))
    }

    fn concat_packed_polygons_many(
        arrays: &[&Self],
        first: &Self,
        first_coords: &CoordSeq,
    ) -> PyResult<Option<Self>> {
        let mut vertex_count = 0_usize;
        let mut ring_count = 0_usize;
        for array in arrays {
            let GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) = array.storage()
            else {
                return Ok(None);
            };
            if coords.axes() != first_coords.axes() {
                return Ok(None);
            }
            vertex_count = vertex_count
                .checked_add(packed_polygons_coord_len(
                    ring_offsets,
                    polygon_offsets,
                    row_map.as_deref(),
                ))
                .ok_or_else(|| {
                    pyo3::exceptions::PyOverflowError::new_err("coordinate count overflows usize")
                })?;
            ring_count = ring_count
                .checked_add(packed_polygons_ring_len(
                    polygon_offsets,
                    row_map.as_deref(),
                ))
                .ok_or_else(|| {
                    pyo3::exceptions::PyOverflowError::new_err("ring count overflows usize")
                })?;
        }
        let mut coord_builder =
            PointColumnBuilder::try_with_capacity(first_coords.axes(), vertex_count)?;
        let mut ring_builder = CsrOffsetBuilder::new();
        let mut polygon_builder = CsrOffsetBuilder::new();
        let mut output_rings = 0_usize;
        for array in arrays {
            let GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) = array.storage()
            else {
                unreachable!("storage kind checked in reservation pass");
            };
            let map = row_map.as_deref();
            for logical in 0..array.len() {
                let rings = map.csr_window(polygon_offsets, logical);
                for ring in rings {
                    coord_builder.extend_window(
                        coords,
                        ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize,
                    )?;
                    output_rings = output_rings.checked_add(1).ok_or_else(|| {
                        pyo3::exceptions::PyOverflowError::new_err("ring count overflows usize")
                    })?;
                    ring_builder.push_end(coord_builder.len(), vertex_count)?;
                }
                polygon_builder.push_end(output_rings, ring_count)?;
            }
        }
        let coords = coord_builder.finish()?;
        let ring_offsets = ring_builder.finish(vertex_count)?.cast_level::<RingLevel>();
        let polygon_offsets = polygon_builder
            .finish(ring_count)?
            .cast_level::<PolygonLevel>();
        Ok(Some(Self::packed_polygons(
            coords,
            ring_offsets,
            polygon_offsets,
            first.frame.clone(),
        )))
    }

    /// Concatenate homogeneous packed arrays in one pass with one exact
    /// reservation per ordinate/offset column. Returns `None` when storage
    /// kinds or coordinate axes differ so the caller can take the Mixed lane.
    pub(crate) fn concat_packed_many(arrays: &[&Self]) -> PyResult<Option<Self>> {
        let Some(first) = arrays.first() else {
            return Ok(None);
        };
        if arrays.iter().any(|array| array.is_compact_nullable()) {
            return Ok(None);
        }
        match first.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Points {
                coords: first_coords,
                ..
            }) => Self::concat_packed_points_many(arrays, first, first_coords),
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords: first_coords,
                ..
            }) => Self::concat_packed_lines_many(arrays, first, first_coords),
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords: first_coords,
                ..
            }) => Self::concat_packed_polygons_many(arrays, first, first_coords),
            GeometryArrayStorage::Mixed(_) => Ok(None),
        }
    }

    /// Coordinate payload for a contiguous logical row range without building
    /// a selected `GeometryArray` or composing a row map.
    pub(crate) fn logical_coordinate_bytes_range(&self, rows: std::ops::Range<usize>) -> usize {
        debug_assert!(rows.end <= self.len());
        match self.storage() {
            GeometryArrayStorage::Mixed(shapes) => {
                shapes[rows].iter().map(Shape::coordinate_bytes).sum()
            },
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) => {
                coords.axes().byte_width(rows.len())
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                let vertices = rows
                    .map(|logical| map.csr_window(offsets, logical).len())
                    .sum();
                coords.axes().byte_width(vertices)
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                let vertices = rows
                    .map(|logical| {
                        let rings = map.csr_window(polygon_offsets, logical);
                        if rings.is_empty() {
                            0
                        } else {
                            ring_offsets[rings.end] as usize - ring_offsets[rings.start] as usize
                        }
                    })
                    .sum();
                coords.axes().byte_width(vertices)
            },
        }
    }

    /// Logical heap footprint for a contiguous logical row range without
    /// materializing its row-selection metadata.
    pub(crate) fn logical_heap_bytes_range(&self, rows: std::ops::Range<usize>) -> usize {
        let coordinate_bytes = self.logical_coordinate_bytes_range(rows.clone());
        match self.storage() {
            GeometryArrayStorage::Mixed(_) => {
                coordinate_bytes + rows.len() * std::mem::size_of::<Shape>()
            },
            GeometryArrayStorage::Packed(PackedStorage::Points { row_map, .. }) => {
                coordinate_bytes + selected_row_map_heap_bytes(row_map.as_deref(), rows)
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines { row_map, .. }) => {
                coordinate_bytes
                    + (rows.len() + 1) * std::mem::size_of::<i32>()
                    + selected_row_map_heap_bytes(row_map.as_deref(), rows)
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                polygon_offsets,
                row_map,
                ..
            }) => {
                let map = row_map.as_deref();
                let rings: usize = rows
                    .clone()
                    .map(|logical| map.csr_window(polygon_offsets, logical).len())
                    .sum();
                coordinate_bytes
                    + (rings + 1) * std::mem::size_of::<i32>()
                    + (rows.len() + 1) * std::mem::size_of::<i32>()
                    + selected_row_map_heap_bytes(map, rows)
            },
        }
    }

    pub(crate) fn materialize_packed_points_parts<'a>(
        &self,
        coords: &CoordSeq,
        row_map: impl Into<RowSelectionRef<'a>>,
    ) -> PyResult<Self> {
        let row_map = row_map.into();
        let len = point_logical_len(coords, row_map);
        let mut builder = PointColumnBuilder::try_with_capacity(coords.axes(), len)?;
        for logical in 0..len {
            builder.push_at(coords, resolve_physical_row(row_map, LogicalRow(logical)).0)?;
        }
        Ok(Self::packed_points(builder.finish()?, self.frame.clone()))
    }

    /// Materialize packed `Points` storage when `row_map` reorders rows.
    pub(crate) fn materialize_packed_points(&self) -> PyResult<Self> {
        if let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
            self.storage()
            && row_map.reorders()
        {
            return self.materialize_packed_points_parts(coords, row_map.as_deref());
        }
        Ok(self.clone())
    }

    /// Zero-copy logical row reorder/subset via `row_map` composition.
    pub(crate) fn remap_packed_lines(
        &self,
        coords: Arc<CoordSeq>,
        offsets: CsrOffsetColumn,
        row_map: &RowSelection,
        rows: impl Iterator<Item = usize>,
    ) -> Self {
        let physical_len = offsets.len().saturating_sub(1);
        let row_map = row_selection_from_typed_logical_rows(
            row_map.as_deref(),
            physical_len,
            rows.map(LogicalRow),
        );
        Self::from_storage(
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }),
            self.frame.clone(),
        )
    }

    /// Gather logical packed-line rows into fresh CSR columns in logical order,
    /// resolving each through `row_map` when present.
    pub(crate) fn materialize_packed_lines_parts<'a>(
        &self,
        coords: &CoordSeq,
        offsets: &[i32],
        row_map: impl Into<RowSelectionRef<'a>>,
    ) -> PyResult<Self> {
        {
            let selection = row_map.into();
            self.select_packed_lines(
                coords,
                offsets,
                resolve_physical_rows(line_logical_len(offsets, selection), selection),
            )
        }
    }

    /// Materialize packed `Lines` storage when `row_map` reorders rows.
    pub(crate) fn materialize_packed_lines(&self) -> PyResult<Self> {
        if let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords,
            offsets,
            row_map,
        }) = self.storage()
            && row_map.reorders()
        {
            return self.materialize_packed_lines_parts(coords, offsets, row_map.as_deref());
        }
        Ok(self.clone())
    }

    /// Zero-copy logical row reorder/subset via `row_map` composition.
    pub(crate) fn remap_packed_polygons(
        &self,
        coords: Arc<CoordSeq>,
        ring_offsets: CsrOffsetColumn<RingLevel>,
        polygon_offsets: CsrOffsetColumn<PolygonLevel>,
        row_map: &RowSelection,
        rows: impl Iterator<Item = usize>,
    ) -> Self {
        let physical_len = polygon_offsets.len().saturating_sub(1);
        let row_map = row_selection_from_typed_logical_rows(
            row_map.as_deref(),
            physical_len,
            rows.map(LogicalRow),
        );
        Self::from_storage(
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }),
            self.frame.clone(),
        )
    }

    /// Gather logical packed-polygon rows into fresh two-level CSR columns in
    /// logical order, resolving each through `row_map` when present.
    pub(crate) fn materialize_packed_polygons_parts<'a>(
        &self,
        coords: &CoordSeq,
        ring_offsets: &[i32],
        polygon_offsets: &[i32],
        row_map: impl Into<RowSelectionRef<'a>>,
    ) -> PyResult<Self> {
        self.select_packed_polygons(coords, ring_offsets, polygon_offsets, {
            let selection = row_map.into();
            resolve_physical_rows(polygon_logical_len(polygon_offsets, selection), selection)
        })
    }

    /// Materialize packed `Polygons` storage when `row_map` reorders rows.
    pub(crate) fn materialize_packed_polygons(&self) -> PyResult<Self> {
        if let GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        }) = self.storage()
            && row_map.reorders()
        {
            return self.materialize_packed_polygons_parts(
                coords,
                ring_offsets,
                polygon_offsets,
                row_map.as_deref(),
            );
        }
        Ok(self.clone())
    }

    /// Gather selected packed-line rows into fresh CSR columns — physical
    /// CSR indices only (does not consult `row_map`). The column-direct core
    /// of `take`/`filter`/`__getitem__` slice over `Lines` storage when the
    /// iterator already carries physical indices (no row boxing, no re-pack
    /// scan).
    pub(crate) fn select_packed_lines(
        &self,
        coords: &CoordSeq,
        offsets: &[i32],
        rows: impl Iterator<Item = PhysicalRow>,
    ) -> PyResult<Self> {
        let mut builder = PackedColumnBuilder::try_like(coords, 0)?;
        for row in rows {
            let row = row.0;
            let window = offsets[row] as usize..offsets[row + 1] as usize;
            builder.push_window(coords, window)?;
        }
        let cap = builder.vertex_len();
        let (out_coords, out_offsets) = builder.finish(cap)?;
        Ok(Self::packed_lines(
            out_coords,
            out_offsets,
            self.frame.clone(),
        ))
    }

    pub(crate) fn concat_packed_polygons(
        &self,
        left: &CoordSeq,
        left_rings: &CsrOffsetColumn<RingLevel>,
        left_polygons: &CsrOffsetColumn<PolygonLevel>,
        right: &CoordSeq,
        right_rings: &CsrOffsetColumn<RingLevel>,
        right_polygons: &CsrOffsetColumn<PolygonLevel>,
    ) -> PyResult<Self> {
        let out_coords = concat_coord_columns(left, right)?;
        let out_ring_offsets = CsrOffsetColumn::<RingLevel>::rebase_concat_validated(
            left_rings,
            right_rings,
            out_coords.len(),
        )?;
        let ring_count = out_ring_offsets.len().saturating_sub(1);
        let out_polygon_offsets = CsrOffsetColumn::<PolygonLevel>::rebase_concat_validated(
            left_polygons,
            right_polygons,
            ring_count,
        )?;
        Ok(Self::packed_polygons(
            out_coords,
            out_ring_offsets,
            out_polygon_offsets,
            self.frame.clone(),
        ))
    }

    /// Gather selected packed-polygon rows into fresh two-level CSR columns —
    /// the column-direct core of `take`/`filter` over `Polygons` storage.
    pub(crate) fn select_packed_polygons(
        &self,
        coords: &CoordSeq,
        ring_offsets: &[i32],
        polygon_offsets: &[i32],
        rows: impl Iterator<Item = PhysicalRow>,
    ) -> PyResult<Self> {
        let mut builder = PackedColumnBuilder::try_like(coords, 0)?;
        let mut polygon_builder = CsrOffsetBuilder::new();
        let mut ring_index = 0_usize;
        for row in rows {
            let row = row.0;
            let ring_start = polygon_offsets[row] as usize;
            let ring_end = polygon_offsets[row + 1] as usize;
            for ring in ring_start..ring_end {
                let window = ring_offsets[ring] as usize..ring_offsets[ring + 1] as usize;
                builder.push_window(coords, window)?;
                ring_index += 1;
            }
            polygon_builder.push_end(ring_index, ring_index)?;
        }
        let cap = builder.vertex_len();
        let (out_coords, out_ring_offsets) = builder.finish(cap)?;
        let out_ring_offsets = out_ring_offsets.cast_level::<RingLevel>();
        let out_polygon_offsets = polygon_builder
            .finish(out_ring_offsets.len().saturating_sub(1))?
            .cast_level::<PolygonLevel>();
        Ok(Self::packed_polygons(
            out_coords,
            out_ring_offsets,
            out_polygon_offsets,
            self.frame.clone(),
        ))
    }

    pub(crate) fn simplify_vw_packed_lines(
        &self,
        coords: &CoordSeq,
        offsets: &[i32],
        tolerance: f64,
    ) -> PyResult<Self> {
        self.simplify_packed_lines_mask(coords, offsets, |xs, ys, keep| {
            // Distance-scale threshold; area form is derived after framing.
            crate::geometry::vw_keep(xs, ys, tolerance, keep)
        })
    }

    pub(crate) fn simplify_dp_packed_lines(
        &self,
        coords: &CoordSeq,
        offsets: &[i32],
        tolerance: f64,
    ) -> PyResult<Self> {
        // One batch-local DP work stack reused across rows (clear+push, no
        // per-row allocation). Free-threading: not a receiver cache / lock.
        let mut stack: Vec<(usize, usize)> = Vec::new();
        self.simplify_packed_lines_mask(coords, offsets, |xs, ys, keep| {
            crate::geometry::rdp_keep(xs, ys, tolerance, keep, &mut stack)
        })
    }

    /// Douglas-Peucker / VW over packed lines, CSR to CSR: one keep-mask per
    /// row window, kept vertices appended into one new column set.
    pub(crate) fn simplify_packed_lines_mask(
        &self,
        coords: &CoordSeq,
        offsets: &[i32],
        mut keep_mask: impl FnMut(&[f64], &[f64], &mut Vec<bool>) -> Option<usize>,
    ) -> PyResult<Self> {
        let (xs, ys) = (coords.xs(), coords.ys());
        let rows = offsets.len() - 1;
        let mut builder = PackedColumnBuilder::try_like(coords, xs.len())?;
        let mut keep: Vec<bool> = Vec::new();
        for row in 0..rows {
            let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
            let window = start..end;
            let kept = keep_mask(
                column_window(xs, &window),
                column_window(ys, &window),
                &mut keep,
            );
            // Too short or nothing removable: the row passes through whole.
            if kept.is_none_or(|count| count == end - start) {
                builder.push_window(coords, window)?;
                continue;
            }
            for (slot, &kept) in keep.iter().enumerate() {
                if kept {
                    builder.push_vertex(coords, start + slot)?;
                }
            }
            builder.close_row()?;
        }
        let cap = builder.vertex_len();
        let (out_coords, out_offsets) = builder.finish(cap)?;
        Ok(Self::packed_lines(
            out_coords,
            out_offsets,
            self.frame.clone(),
        ))
    }

    /// Shared curve-key engine: one frame for the whole array (explicit
    /// bounds or total bounds), one key per row. Empty and missing rows use
    /// `u64::MAX`, the documented sort-last sentinel.
    pub(crate) fn curve_keys(
        &self,
        level: i64,
        bounds: Option<&Bound<'_, PyAny>>,
        _operation: &'static str,
        kind: curves::CurveKind,
    ) -> PyResult<Vec<u64>> {
        // Same level validation + CurveKind::key as the scalar spatial_key
        // kernel; only the frame source differs (total bounds vs per-shape).
        let level = crate::boundary::input::validate_curve_level(level)?;
        let bounds = match parse_curve_bounds(bounds)? {
            Some(bounds) => Some(bounds),
            None if self.has_missing() => self.try_drop_missing()?.storage().total_bounds(),
            None => self.storage().total_bounds(),
        };
        if self.len() == 0 {
            return Ok(Vec::new());
        }
        let Some(bounds) = bounds else {
            let mut keys = crate::try_vec_with_capacity(self.len())?;
            for _ in 0..self.len() {
                keys.push(u64::MAX);
            }
            return Ok(keys);
        };
        let frame = curves::CurveFrame::new(bounds, level);
        let mut keys = crate::try_vec_with_capacity(self.len())?;
        for row in self.row_operation().rows_with_missing(|| u64::MAX) {
            let key = match row {
                ArrayRow::Missing { value, .. } => value,
                ArrayRow::Present(row) => row
                    .row()
                    .with_shape(|shape| kind.key(&frame, shape).unwrap_or(u64::MAX))?,
            };
            keys.push(key);
        }
        Ok(keys)
    }

    /// The array reordered by ascending `keys` (stable, so equal keys keep
    /// row order).
    pub(crate) fn taken_by_keys(&self, keys: &[u64]) -> crate::error::Result<Self> {
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.sort_by_key(|&row| (self.is_row_missing(row), keys[row]));
        self.gather_logical_rows(&order)
    }

    pub fn items(&self) -> PyResult<std::borrow::Cow<'_, [PyGeometry]>> {
        // Mixed no longer stores PyGeometry; materialize through the array
        // geometry_at cache so repeated items()/arr[i] share ShapeData.
        let mut items = crate::try_vec_with_capacity(self.len())?;
        for index in 0..self.len() {
            items.push(self.geometry_at(index).map_err(crate::PyErr::from)?);
        }
        Ok(std::borrow::Cow::Owned(items))
    }

    /// Map each row's shape to a `Vec<Shape>` and collect into a per-row
    /// [`Groups`](crate::py::vectors::Groups): each input row's outputs form one
    /// ragged group — missing rows yield empty groups — so which outputs came
    /// from which input is preserved. One detached pass builds the flat values
    /// and the per-row `offsets` together.
    pub(crate) fn flat_map_shapes_groups(
        &self,
        py: Python<'_>,
        transform: impl Fn(&Shape) -> Result<Vec<Shape>> + Send + Sync,
    ) -> PyResult<crate::py::vectors::Groups> {
        self.flat_map_shapes_groups_indexed(py, move |_, shape| transform(shape))
    }

    pub(crate) fn flat_map_shapes_groups_indexed(
        &self,
        py: Python<'_>,
        transform: impl Fn(usize, &Shape) -> Result<Vec<Shape>> + Send + Sync,
    ) -> PyResult<crate::py::vectors::Groups> {
        let array = self.clone();
        let frame = self.frame.clone();
        let builder = py
            .detach(move || {
                let mut builder = crate::CsrBuilder::<Shape>::new();
                for logical_row in array.row_operation().rows_with_missing(Vec::new) {
                    let (row, values) = match logical_row {
                        ArrayRow::Missing { logical, value } => (logical, value),
                        ArrayRow::Present(row) => {
                            let logical = row.logical();
                            let values = row
                                .row()
                                .try_with_shape(|shape| transform(logical, shape))
                                .map_err(|error| (logical, error))?;
                            (logical, values)
                        },
                    };
                    builder.push_row(values).map_err(|error| (row, error))?;
                }
                Ok::<_, (usize, crate::error::Error)>(builder)
            })
            .map_err(rows_err)?;
        crate::py::vectors::Groups::from_geometry_builder(frame, builder)
    }

    /// Budgeted group flattening for operations that synthesize a variable
    /// number of geometries.  One detached row walk owns the counter across
    /// mixed rows, missing masks, and every output group.
    pub(crate) fn flat_map_shapes_groups_budgeted(
        &self,
        py: Python<'_>,
        operation: &'static str,
        parameter: &'static str,
        mut transform: impl FnMut(
            usize,
            &Shape,
            &mut crate::geometry::ExpansionBudget,
        ) -> Result<Vec<Shape>>
        + Send,
    ) -> PyResult<crate::py::vectors::Groups> {
        let array = self.clone();
        let frame = self.frame.clone();
        let builder = py
            .detach(move || {
                let mut budget = crate::geometry::ExpansionBudget::new(operation, parameter);
                let mut builder = crate::CsrBuilder::<Shape>::new();
                for logical_row in array.row_operation().rows_with_missing(Vec::new) {
                    let (row, values) = match logical_row {
                        ArrayRow::Missing { logical, value } => (logical, value),
                        ArrayRow::Present(row) => {
                            let logical = row.logical();
                            let values = row
                                .row()
                                .try_with_shape(|shape| transform(logical, shape, &mut budget))
                                .map_err(|error| (logical, error))?;
                            (logical, values)
                        },
                    };
                    builder.push_row(values).map_err(|error| (row, error))?;
                }
                Ok::<_, (usize, crate::error::Error)>(builder)
            })
            .map_err(rows_err)?;
        crate::py::vectors::Groups::from_geometry_builder(frame, builder)
    }

    /// Packed triangle analogue of [`flat_map_shapes_groups_budgeted`].  The
    /// same array-wide budget is charged by the triangle vertex emitter before
    /// its columns are appended.
    pub(crate) fn flat_map_packed_triangles_groups_budgeted(
        &self,
        py: Python<'_>,
        operation: &'static str,
        parameter: &'static str,
        mut transform: impl FnMut(
            usize,
            &Shape,
            &mut crate::geometry::ExpansionBudget,
        ) -> Result<Vec<Point>>
        + Send,
    ) -> PyResult<crate::py::vectors::Groups> {
        let array = self.clone();
        let frame = self.frame.clone();
        let builder = py
            .detach(move || {
                let mut budget = crate::geometry::ExpansionBudget::new(operation, parameter);
                let mut builder = crate::CsrBuilder::<Point>::new();
                for logical_row in array.row_operation().rows_with_missing(Vec::new) {
                    let (row, values) = match logical_row {
                        ArrayRow::Missing { logical, value } => (logical, value),
                        ArrayRow::Present(row) => {
                            let logical = row.logical();
                            let values = row
                                .row()
                                .try_with_shape(|shape| transform(logical, shape, &mut budget))
                                .map_err(|error| (logical, error))?;
                            (logical, values)
                        },
                    };
                    if values.len() % 4 != 0 {
                        return Err((
                            row,
                            crate::geometry::GeometryErrorKind::Invalid(
                                "triangle output is not a multiple of four vertices".into(),
                            )
                            .into(),
                        ));
                    }
                    for value in values {
                        builder.push(value).map_err(|error| (row, error))?;
                    }
                    let end = builder.len() / 4;
                    builder.push_end(end).map_err(|error| (row, error))?;
                }
                Ok::<_, (usize, crate::error::Error)>(builder)
            })
            .map_err(rows_err)?;
        crate::py::vectors::Groups::from_point_builder(frame, builder)
    }

    pub(crate) fn boundary_unary_packed(&self) -> PyResult<Self> {
        if self.is_compact_nullable() {
            return self.map_shapes(|shape| Ok(shape.boundary()?));
        }
        // Hole-free polygons (every polygon is exactly one shell ring): the
        // boundary IS that shell ring as a closed `LineString`. The shell
        // coordinates and their windows are ALREADY in the packed columns, so
        // the result reuses them — share the `coords` Arc and use `ring_offsets`
        // as the line offsets, ZERO coordinate copy and no per-row Shape
        // materialization (the map path's cost). `ring_offsets.len() ==
        // polygon_offsets.len()` exactly when `total_rings == rows`, i.e. one
        // ring per polygon; any holes/empties fall through to the general map.
        if let GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        }) = self.storage()
            && ring_offsets.len() == polygon_offsets.len()
        {
            return Ok(Self::from_storage(
                GeometryArrayStorage::Packed(PackedStorage::Lines {
                    coords: Arc::clone(coords),
                    offsets: ring_offsets.clone().cast_level::<()>(),
                    row_map: row_map.clone(),
                }),
                self.frame.clone(),
            )
            .with_missing_mask(self.missing().cloned())?);
        }
        self.map_shapes(|shape| Ok(shape.boundary()?))
    }

    /// A packed point array's rows as ONE `Shape::MultiPoint` over the shared
    /// coordinate columns — the zero-copy staging for whole-array dissolves
    /// (`dissolve()`): the same kernel and output as N point rows, with no
    /// per-row boxing or shape clones. `None` for non-point storage, masked
    /// arrays, or empty columns (callers fall back to the generic staging).
    pub(crate) fn packed_points_as_multipoint(&self) -> Result<Option<Shape>> {
        if self.has_missing() {
            return Ok(None);
        }
        let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) =
            self.storage()
        else {
            return Ok(None);
        };
        if coords.is_empty() {
            return Ok(None);
        }
        Ok(Some(if row_map.is_identity() {
            Shape::multi_point(coords.axes(), (**coords).clone())
        } else {
            let map = row_map.as_deref();
            let mut points = Vec::new();
            let len = crate::array::point_logical_len(coords, map);
            crate::boundary::try_reserve_vec(&mut points, len)?;
            for logical in 0..len {
                crate::boundary::try_push_vec(
                    &mut points,
                    coords.point_at(crate::array::physical_row(map, logical)),
                )?;
            }
            Shape::MultiPoint(crate::geometry::MultiPoint::from_coordseq(
                CoordSeq::try_from_points(&points)?,
            )?)
        }))
    }

    /// Materialize a borrowed `&[&Shape]` view of every row once (`Cow` borrows
    /// boxed storage, stack points for packed) and run `f` over it — the staging
    /// the n-ary `*_all` kernels need, without restating the double-collect.
    pub(crate) fn with_borrowed_shapes<R>(
        &self,
        f: impl FnOnce(&[&Shape]) -> Result<R>,
    ) -> Result<R> {
        // Aggregates SKIP missing rows (SQL/pandas semantics): stage only the
        // present shapes. Dense arrays stage every row unchanged.
        let mut rows = Vec::new();
        crate::boundary::try_reserve_vec(&mut rows, self.len())?;
        for row in self.present_shape_rows() {
            let (_, shape) = row?;
            crate::boundary::try_push_vec(&mut rows, shape)?;
        }
        let mut shapes = Vec::new();
        crate::boundary::try_reserve_vec(&mut shapes, rows.len())?;
        for shape in &rows {
            crate::boundary::try_push_vec(&mut shapes, shape.as_ref())?;
        }
        f(&shapes)
    }
}

#[path = "map_shapes.rs"]
mod map_shapes;
