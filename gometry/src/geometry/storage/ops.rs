#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use super::{
    GeometryArrayStorage, PackedRow, PackedStorage, RowSelectionRef, RowsIter, ShapeRow,
    ShapesIter, column_window, csr_value_range, csr_value_window, line_logical_len,
    point_logical_len, polygon_logical_len,
};
use crate::error::Result;
use crate::geometry::types::ShapeHeapMemo;
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, CsrOffsetColumn, GeometryKind, Polygon, Shape, storage,
};

impl GeometryArrayStorage {
    pub(crate) const fn uniform_kind(&self) -> Option<crate::geometry::GeometryKind> {
        match self {
            Self::Packed(PackedStorage::Points { .. }) => {
                Some(crate::geometry::GeometryKind::Point)
            },
            Self::Packed(PackedStorage::Lines { .. }) => {
                Some(crate::geometry::GeometryKind::LineString)
            },
            Self::Packed(PackedStorage::Polygons { .. }) => {
                Some(crate::geometry::GeometryKind::Polygon)
            },
            Self::Mixed(_) => None,
        }
    }
    /// The one coordinate layout carried by homogeneous packed storage.
    /// Selections only remap rows and missing slots use placeholders with the
    /// same columns, so this is O(1) for points, lines, and polygons.
    pub(crate) fn packed_axes(&self) -> Option<CoordinateAxes> {
        match self {
            Self::Packed(packed) => Some(packed.axes()),
            Self::Mixed(_) => None,
        }
    }

    pub(crate) fn const_or_shape_bool(
        &self,
        constant: bool,
        shape_pred: impl Fn(&Shape) -> Result<bool>,
    ) -> Result<Vec<bool>> {
        let mut output = Vec::with_capacity(self.len());
        match self {
            Self::Packed(PackedStorage::Points { .. } | PackedStorage::Polygons { .. }) => {
                output.resize(self.len(), constant);
            },
            Self::Mixed(_) | Self::Packed(PackedStorage::Lines { .. }) => {
                for shape in self.iter_shapes() {
                    let shape = shape?;
                    output.push(shape_pred(&shape)?);
                }
            },
        }
        Ok(output)
    }

    pub(crate) fn lines_bool(
        &self,
        line: impl Fn(&CoordSeq) -> Result<bool>,
    ) -> Option<Result<Vec<bool>>> {
        match self {
            Self::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                Some((|| {
                    let len = line_logical_len(offsets.as_slice(), map);
                    let mut output = Vec::with_capacity(len);
                    for logical in 0..len {
                        let row = PackedRow::line(logical, coords, offsets, map);
                        let window = row
                            .coordinate_window()
                            .expect("packed line row has a coordinate window");
                        output.push(line(&coords.view(window))?);
                    }
                    Ok(output)
                })())
            },
            Self::Packed(PackedStorage::Points { .. } | PackedStorage::Polygons { .. })
            | Self::Mixed(_) => None,
        }
    }

    /// Dense polygon-row bool kernel (no missing-mask branch).
    pub(crate) fn polygons_bool(
        &self,
        polygon: impl Fn(Option<&Polygon>) -> Result<bool>,
    ) -> Option<Result<Vec<bool>>> {
        match self {
            Self::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                let coords = coords.as_ref();
                let ring_offsets = ring_offsets.as_slice();
                let polygon_offsets = polygon_offsets.as_slice();
                Some((|| {
                    let len = polygon_logical_len(polygon_offsets, map);
                    let mut output = Vec::with_capacity(len);
                    for logical in 0..len {
                        let rings = map.csr_window(polygon_offsets, logical);
                        let polygon_value = crate::geometry::storage::packed_polygon_view!(
                            coords,
                            ring_offsets,
                            rings
                        )?;
                        output.push(polygon(polygon_value.as_ref())?);
                    }
                    Ok(output)
                })())
            },
            Self::Packed(PackedStorage::Points { .. } | PackedStorage::Lines { .. })
            | Self::Mixed(_) => None,
        }
    }

    /// Like [`lines_bool`], but writes `false` for missing rows without
    /// evaluating the kernel on NaN placeholders. Call only when a missing
    /// mask is present — dense input uses [`lines_bool`].
    pub(crate) fn lines_bool_masked(
        &self,
        missing: Option<&storage::ValidityMask>,
        line: impl Fn(&CoordSeq) -> Result<bool>,
    ) -> Option<Result<Vec<bool>>> {
        let Some(mask) = missing else {
            return self.lines_bool(line);
        };
        match self {
            Self::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                Some((|| {
                    let len = line_logical_len(offsets.as_slice(), map);
                    let mut output = Vec::with_capacity(len);
                    for logical in 0..len {
                        let value = if mask.is_missing(logical) {
                            false
                        } else {
                            let row = PackedRow::line(logical, coords, offsets, map);
                            let window = row
                                .coordinate_window()
                                .expect("packed line row has a coordinate window");
                            line(&coords.view(window))?
                        };
                        output.push(value);
                    }
                    Ok(output)
                })())
            },
            Self::Packed(PackedStorage::Points { .. } | PackedStorage::Polygons { .. })
            | Self::Mixed(_) => None,
        }
    }

    /// Polygon-row bool kernel; writes `false` for missing rows without
    /// evaluating on NaN placeholders. Call only when a missing mask is
    /// present — dense input uses [`polygons_bool`].
    pub(crate) fn polygons_bool_masked(
        &self,
        missing: Option<&storage::ValidityMask>,
        polygon: impl Fn(Option<&Polygon>) -> Result<bool>,
    ) -> Option<Result<Vec<bool>>> {
        let Some(mask) = missing else {
            return self.polygons_bool(polygon);
        };
        match self {
            Self::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                let coords = coords.as_ref();
                let ring_offsets = ring_offsets.as_slice();
                let polygon_offsets = polygon_offsets.as_slice();
                Some((|| {
                    let len = polygon_logical_len(polygon_offsets, map);
                    let mut output = Vec::with_capacity(len);
                    for logical in 0..len {
                        let value = if mask.is_missing(logical) {
                            false
                        } else {
                            let rings = map.csr_window(polygon_offsets, logical);
                            let polygon_value = crate::geometry::storage::packed_polygon_view!(
                                coords,
                                ring_offsets,
                                rings
                            )?;
                            polygon(polygon_value.as_ref())?
                        };
                        output.push(value);
                    }
                    Ok(output)
                })())
            },
            Self::Packed(PackedStorage::Points { .. } | PackedStorage::Lines { .. })
            | Self::Mixed(_) => None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Mixed(items) => items.len(),
            Self::Packed(PackedStorage::Points { coords, row_map }) => {
                point_logical_len(coords, row_map.as_deref())
            },
            Self::Packed(PackedStorage::Lines {
                offsets, row_map, ..
            }) => line_logical_len(offsets.as_slice(), row_map.as_deref()),
            Self::Packed(PackedStorage::Polygons {
                polygon_offsets,
                row_map,
                ..
            }) => polygon_logical_len(polygon_offsets.as_slice(), row_map.as_deref()),
        }
    }

    pub(crate) fn logical_heap_bytes(&self) -> usize {
        let owner = crate::heap_size::ARC_CONTROL_BYTES.saturating_add(std::mem::size_of::<Self>());
        owner.saturating_add(match self {
            // Shape payload + the Vec's own allocation. Prepared ShapeData
            // (array-side cache) is counted on PyGeometryArray::heap_bytes
            // once chunks initialize — see RowProducts::heap_bytes.
            Self::Mixed(shapes) => {
                let mut memo = ShapeHeapMemo::default();
                let nested = shapes.iter().fold(0_usize, |bytes, shape| {
                    bytes.saturating_add(memo.heap_bytes(shape))
                });
                shapes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Shape>())
                    .saturating_add(nested)
            },
            Self::Packed(PackedStorage::Points { coords, row_map }) => {
                let rows = point_logical_len(coords, row_map.as_deref());
                crate::heap_size::ARC_CONTROL_BYTES
                    .saturating_add(std::mem::size_of::<CoordSeq>())
                    .saturating_add(coords.logical_heap_bytes_for_count_saturating(rows))
                    .saturating_add(row_map.heap_bytes())
            },
            Self::Packed(PackedStorage::Lines {
                coords,
                offsets: offsets_column,
                row_map,
            }) => {
                let rows = line_logical_len(offsets_column.as_slice(), row_map.as_deref());
                let map = row_map.as_deref();
                let offsets = offsets_column.as_slice();
                let vertices = csr_value_range(offsets, map, 0..rows).map_or_else(
                    || {
                        (0..rows).fold(0_usize, |count, row| {
                            #[cfg(test)]
                            storage::record_csr_size_row_visit();
                            count.saturating_add(map.csr_window(offsets, row).len())
                        })
                    },
                    |vertices| vertices.len(),
                );
                crate::heap_size::ARC_CONTROL_BYTES
                    .saturating_add(std::mem::size_of::<CoordSeq>())
                    .saturating_add(coords.logical_heap_bytes_for_count_saturating(vertices))
                    .saturating_add(
                        offsets_column.logical_heap_bytes_for_count(rows.saturating_add(1)),
                    )
                    .saturating_add(row_map.heap_bytes())
            },
            Self::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets: ring_offsets_column,
                polygon_offsets: polygon_offsets_column,
                row_map,
            }) => {
                let polygons = polygon_offsets_column.as_slice();
                let ring_offsets = ring_offsets_column.as_slice();
                let map = row_map.as_deref();
                let rows = polygon_logical_len(polygons, map);
                let (rings, vertices) = csr_value_range(polygons, map, 0..rows).map_or_else(
                    || {
                        (0..rows).fold((0_usize, 0_usize), |(ring_count, vertex_count), row| {
                            #[cfg(test)]
                            storage::record_csr_size_row_visit();
                            let row_rings = map.csr_window(polygons, row);
                            if row_rings.is_empty() {
                                (ring_count, vertex_count)
                            } else {
                                (
                                    ring_count.saturating_add(row_rings.len()),
                                    vertex_count.saturating_add(
                                        csr_value_window(ring_offsets, row_rings).len(),
                                    ),
                                )
                            }
                        })
                    },
                    |rings| {
                        let vertex_count = if rings.is_empty() {
                            0
                        } else {
                            csr_value_range(ring_offsets, RowSelectionRef::Identity, rings.clone())
                                .expect("admitted ring offsets include the selected endpoints")
                                .len()
                        };
                        (rings.len(), vertex_count)
                    },
                );
                crate::heap_size::ARC_CONTROL_BYTES
                    .saturating_add(std::mem::size_of::<CoordSeq>())
                    .saturating_add(coords.logical_heap_bytes_for_count_saturating(vertices))
                    .saturating_add(
                        ring_offsets_column.logical_heap_bytes_for_count(rings.saturating_add(1)),
                    )
                    .saturating_add(
                        polygon_offsets_column.logical_heap_bytes_for_count(rows.saturating_add(1)),
                    )
                    .saturating_add(row_map.heap_bytes())
            },
        })
    }

    pub(crate) fn line_view<'a>(
        coords: &CoordSeq,
        offsets: &CsrOffsetColumn,
        row_map: impl Into<RowSelectionRef<'a>>,
        logical: usize,
    ) -> CoordSeq {
        let row = PackedRow::line(logical, coords, offsets, row_map.into());
        coords.view(
            row.coordinate_window()
                .expect("packed line row has a coordinate window"),
        )
    }

    /// Raw axis-aligned coordinate bounds over all geometries, or ``None`` if
    /// every geometry is empty. Geographic public bounds are resolved by the
    /// array owner, which can interpret periodic support.
    ///
    /// Returns
    /// -------
    /// tuple or None
    pub(crate) fn raw_total_bounds(&self) -> Option<Bounds> {
        if let Self::Packed(PackedStorage::Polygons { row_map, .. }) = self
            && row_map.is_identity()
        {
            let mut total: Option<Bounds> = None;
            for row in 0..self.len() {
                let Some(bounds) = self.row(row).quick_bounds() else {
                    continue;
                };
                match &mut total {
                    Some(current) => current.include_bounds(bounds),
                    None => total = Some(bounds),
                }
            }
            return total;
        }
        let packed = match self {
            Self::Packed(
                PackedStorage::Points { coords, row_map }
                | PackedStorage::Lines {
                    coords, row_map, ..
                },
            ) => row_map.is_identity().then_some(coords),
            Self::Packed(PackedStorage::Polygons { .. }) | Self::Mixed(_) => None,
        };
        if let Some(seq) = packed {
            let (minx, maxx) = crate::geometry::column_minmax(seq.xs())?;
            let (miny, maxy) = crate::geometry::column_minmax(seq.ys())?;
            return Some(Bounds::new_raw(minx, miny, maxx, maxy));
        }
        let mut values = self.iter_rows().filter_map(ShapeRow::quick_bounds);
        let first = values.next()?;
        Some(values.fold(first, |mut acc, bounds| {
            acc.include_bounds(bounds);
            acc
        }))
    }

    pub(crate) fn envelope_box_shapes(&self) -> Result<Option<Vec<Shape>>> {
        match self {
            Self::Packed(PackedStorage::Polygons { .. } | PackedStorage::Lines { .. }) => {
                let mut output = Vec::with_capacity(self.len());
                self.try_for_each_element_bounds(|bounds| {
                    output.push(bounds.map_or_else(
                        || Ok(Shape::empty_polygon()),
                        crate::geometry::bounds_to_shape,
                    )?);
                    Ok::<_, crate::error::Error>(())
                })?;
                Ok(Some(output))
            },
            Self::Packed(PackedStorage::Points { .. }) | Self::Mixed(_) => Ok(None),
        }
    }

    /// Visit each stored row's planar bounds in row order, straight off the
    /// packed columns: `None` for an empty row or a non-finite point. Mixed
    /// storage visits nothing and returns `false`.
    pub(crate) fn try_for_each_element_bounds<E>(
        &self,
        mut visit: impl FnMut(Option<Bounds>) -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E> {
        match self {
            Self::Packed(_) => {
                for logical in 0..self.len() {
                    visit(self.row(logical).quick_bounds())?;
                }
            },
            Self::Mixed(_) => return Ok(false),
        }
        Ok(true)
    }

    pub(crate) fn iter_shapes(&self) -> ShapesIter<'_> {
        ShapesIter::new(self)
    }

    pub(crate) fn iter_rows(&self) -> RowsIter<'_> {
        RowsIter::new(self)
    }

    /// Borrow one logical row without materializing a `Shape`.
    ///
    /// This is the random-access counterpart to [`Self::iter_rows`]. Delegate
    /// packed selectors and CSR windows to [`PackedRow`] so consumers such as
    /// the spatial index cannot grow a second interpretation of storage.
    #[expect(
        clippy::indexing_slicing,
        reason = "mixed logical indices are admitted by the array row operation; retain O(1) random access"
    )]
    pub(crate) fn row(&self, index: usize) -> ShapeRow<'_> {
        match self {
            Self::Mixed(shapes) => ShapeRow::Shape(&shapes[index]),
            Self::Packed(packed) => ShapeRow::from_packed(PackedRow::from_storage(packed, index)),
        }
    }
}

pub(crate) fn reverse_coord_windows(coords: &CoordSeq, boundaries: &[i32]) -> Result<CoordSeq> {
    let partitioned = csr_windows_partition(boundaries, coords.len());
    let reverse_column = |column: &[f64]| {
        let len = column.len();
        let mut output = Vec::with_capacity(len);
        if !partitioned {
            output.extend(column.iter().rev().copied());
            return output;
        }
        for &[start, end] in boundaries.array_windows::<2>() {
            let window = start as usize..end as usize;
            output.extend(column_window(column, &window).iter().rev().copied());
        }
        output
    };
    CoordSeq::try_from_vecs(
        reverse_column(coords.xs()),
        reverse_column(coords.ys()),
        coords.zs().map(reverse_column),
        coords.ms().map(reverse_column),
    )
}

/// True when `boundaries` is a CSR offset table that partitions `0..len`
/// into contiguous half-open windows (starts at 0, ends at `len`, each
/// window `start <= end`, adjacent windows abut).
fn csr_windows_partition(boundaries: &[i32], len: usize) -> bool {
    match boundaries {
        [] => len == 0,
        // A single offset is not a window; need at least start+end.
        [_] => false,
        [first, ..] => {
            if *first != 0 {
                return false;
            }
            let mut covered = 0_usize;
            for &[start, end] in boundaries.array_windows::<2>() {
                if start as usize != covered || end < start {
                    return false;
                }
                let end = end as usize;
                if end > len {
                    return false;
                }
                covered = end;
            }
            covered == len
        },
    }
}

impl PackedStorage {
    pub(crate) const fn kind(&self) -> GeometryKind {
        match self {
            Self::Points { .. } => GeometryKind::Point,
            Self::Lines { .. } => GeometryKind::LineString,
            Self::Polygons { .. } => GeometryKind::Polygon,
        }
    }

    pub(crate) fn axes(&self) -> CoordinateAxes {
        match self {
            Self::Points { coords, .. }
            | Self::Lines { coords, .. }
            | Self::Polygons { coords, .. } => coords.axes(),
        }
    }
}
