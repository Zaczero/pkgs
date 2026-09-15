#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::HeapSize as _;
use crate::array::{
    Bounds, CoordSeq, CoordinateAxes, GeometryArrayStorage, LineRows, PackedStorage, PointRows,
    Polygon, Ring, RowSelectionRef, RowsIter, Shape, ShapeRow, ShapesIter, column_window,
    line_logical_len, packed_lines_coord_len, packed_polygons_coord_len, packed_polygons_ring_len,
    physical_row, point_logical_len, polygon_logical_len, polygon_rings_range,
};
use crate::error::Result;
use crate::geometry::CoordWindow;

const fn coordseq_logical_bytes(coords: &CoordSeq, count: usize) -> usize {
    coords.axes().byte_width(count)
}

impl GeometryArrayStorage {
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
        let mut output = Vec::new();
        crate::boundary::try_reserve_vec(&mut output, self.len())?;
        match self {
            Self::Packed(PackedStorage::Points { .. } | PackedStorage::Polygons { .. }) => {
                for _ in 0..self.len() {
                    crate::boundary::try_push_vec(&mut output, constant)?;
                }
            },
            _ => {
                for shape in self.iter_shapes() {
                    let shape = shape?;
                    crate::boundary::try_push_vec(&mut output, shape_pred(&shape)?)?;
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
                    let mut output = Vec::new();
                    crate::boundary::try_reserve_vec(&mut output, len)?;
                    for logical in 0..len {
                        let window = map.csr_window(offsets.as_slice(), logical);
                        crate::boundary::try_push_vec(
                            &mut output,
                            line(&coords.view(CoordWindow::from_range(window, coords.len())))?,
                        )?;
                    }
                    Ok(output)
                })())
            },
            _ => None,
        }
    }

    /// Dense polygon-row bool kernel (no missing-mask branch).
    pub(crate) fn polygons_bool(
        &self,
        polygon: impl Fn(&Polygon) -> Result<bool>,
    ) -> Option<Result<Vec<bool>>> {
        match self {
            Self::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                Some((|| {
                    let len = polygon_logical_len(polygon_offsets.as_slice(), map);
                    let mut output = Vec::new();
                    crate::boundary::try_reserve_vec(&mut output, len)?;
                    for logical in 0..len {
                        crate::boundary::try_push_vec(
                            &mut output,
                            polygon(&Self::polygon_view(
                                coords,
                                ring_offsets,
                                polygon_rings_range(polygon_offsets.as_slice(), map, logical),
                            )?)?,
                        )?;
                    }
                    Ok(output)
                })())
            },
            _ => None,
        }
    }

    /// Like [`lines_bool`], but writes `false` for missing rows without
    /// evaluating the kernel on NaN placeholders. Call only when a missing
    /// mask is present — dense input uses [`lines_bool`].
    pub(crate) fn lines_bool_masked(
        &self,
        missing: Option<&crate::array::ValidityMask>,
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
                    let mut output = Vec::new();
                    crate::boundary::try_reserve_vec(&mut output, len)?;
                    for logical in 0..len {
                        let value = if mask.is_missing(logical) {
                            false
                        } else {
                            let window = map.csr_window(offsets.as_slice(), logical);
                            line(&coords.view(CoordWindow::from_range(window, coords.len())))?
                        };
                        crate::boundary::try_push_vec(&mut output, value)?;
                    }
                    Ok(output)
                })())
            },
            _ => None,
        }
    }

    /// Polygon-row bool kernel; writes `false` for missing rows without
    /// evaluating on NaN placeholders. Call only when a missing mask is
    /// present — dense input uses [`polygons_bool`].
    pub(crate) fn polygons_bool_masked(
        &self,
        missing: Option<&crate::array::ValidityMask>,
        polygon: impl Fn(&Polygon) -> Result<bool>,
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
                Some((|| {
                    let len = polygon_logical_len(polygon_offsets.as_slice(), map);
                    let mut output = Vec::new();
                    crate::boundary::try_reserve_vec(&mut output, len)?;
                    for logical in 0..len {
                        let value = if mask.is_missing(logical) {
                            false
                        } else {
                            polygon(&Self::polygon_view(
                                coords,
                                ring_offsets,
                                polygon_rings_range(polygon_offsets.as_slice(), map, logical),
                            )?)?
                        };
                        crate::boundary::try_push_vec(&mut output, value)?;
                    }
                    Ok(output)
                })())
            },
            _ => None,
        }
    }

    pub(crate) fn try_map_line_rows<'a, T>(
        offsets: &[i32],
        row_map: impl Into<RowSelectionRef<'a>>,
        f: impl Fn(std::ops::Range<usize>) -> Result<T>,
    ) -> Result<Vec<T>> {
        let map = row_map.into();
        let len = line_logical_len(offsets, map);
        let mut output = Vec::new();
        crate::boundary::try_reserve_vec(&mut output, len)?;
        for logical in 0..len {
            crate::boundary::try_push_vec(&mut output, f(map.csr_window(offsets, logical))?)?;
        }
        Ok(output)
    }

    pub(crate) fn try_map_polygon_rows<'a, T>(
        ring_offsets: &[i32],
        polygon_offsets: &[i32],
        row_map: impl Into<RowSelectionRef<'a>>,
        all_rings: bool,
        f: impl Fn(Option<std::ops::Range<usize>>) -> Result<T>,
    ) -> Result<Vec<T>> {
        let map = row_map.into();
        let len = polygon_logical_len(polygon_offsets, map);
        let mut output = Vec::new();
        crate::boundary::try_reserve_vec(&mut output, len)?;
        for logical in 0..len {
            let rings = polygon_rings_range(polygon_offsets, map, logical);
            let window = if rings.is_empty() {
                None
            } else if all_rings {
                Some(ring_offsets[rings.start] as usize..ring_offsets[rings.end] as usize)
            } else {
                Some(ring_offsets[rings.start] as usize..ring_offsets[rings.start + 1] as usize)
            };
            crate::boundary::try_push_vec(&mut output, f(window)?)?;
        }
        Ok(output)
    }

    pub fn len(&self) -> usize {
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

    pub(crate) fn logical_coordinate_bytes(&self) -> usize {
        match self {
            Self::Mixed(shapes) => shapes.iter().map(Shape::coordinate_bytes).sum(),
            Self::Packed(PackedStorage::Points { coords, row_map }) => {
                coordseq_logical_bytes(coords, point_logical_len(coords, row_map.as_deref()))
            },
            Self::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => coordseq_logical_bytes(
                coords,
                packed_lines_coord_len(offsets.as_slice(), row_map.as_deref()),
            ),
            Self::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => coordseq_logical_bytes(
                coords,
                packed_polygons_coord_len(
                    ring_offsets.as_slice(),
                    polygon_offsets.as_slice(),
                    row_map.as_deref(),
                ),
            ),
        }
    }

    pub(crate) fn logical_heap_bytes(&self) -> usize {
        let owner = crate::heap_size::ARC_CONTROL_BYTES + std::mem::size_of::<Self>();
        owner
            + match self {
                // Shape payload + the Vec's own allocation. Prepared ShapeData
                // (array-side cache) is counted on PyGeometryArray::heap_bytes
                // once chunks initialize — see RowProducts::heap_bytes.
                Self::Mixed(shapes) => {
                    // `Shape::heap_bytes` includes its coordinate payload for
                    // scalar accounting; subtract that part so this array
                    // adds only nested owners and container allocations.
                    self.logical_coordinate_bytes()
                        + shapes.capacity() * std::mem::size_of::<Shape>()
                        + shapes
                            .iter()
                            .map(|shape| {
                                shape.heap_bytes().saturating_sub(shape.coordinate_bytes())
                            })
                            .sum::<usize>()
                },
                Self::Packed(PackedStorage::Points { coords, row_map }) => {
                    let rows = point_logical_len(coords, row_map.as_deref());
                    crate::heap_size::ARC_CONTROL_BYTES
                        + std::mem::size_of::<CoordSeq>()
                        + coords.logical_heap_bytes_for_count(rows)
                        + row_map.heap_bytes()
                },
                Self::Packed(PackedStorage::Lines {
                    coords,
                    offsets,
                    row_map,
                }) => {
                    let rows = line_logical_len(offsets.as_slice(), row_map.as_deref());
                    let vertices = packed_lines_coord_len(offsets.as_slice(), row_map.as_deref());
                    crate::heap_size::ARC_CONTROL_BYTES
                        + std::mem::size_of::<CoordSeq>()
                        + coords.logical_heap_bytes_for_count(vertices)
                        + offsets.logical_heap_bytes_for_count(rows + 1)
                        + row_map.heap_bytes()
                },
                Self::Packed(PackedStorage::Polygons {
                    coords,
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                }) => {
                    let rows = polygon_logical_len(polygon_offsets.as_slice(), row_map.as_deref());
                    let rings =
                        packed_polygons_ring_len(polygon_offsets.as_slice(), row_map.as_deref());
                    let vertices = packed_polygons_coord_len(
                        ring_offsets.as_slice(),
                        polygon_offsets.as_slice(),
                        row_map.as_deref(),
                    );
                    crate::heap_size::ARC_CONTROL_BYTES
                        + std::mem::size_of::<CoordSeq>()
                        + coords.logical_heap_bytes_for_count(vertices)
                        + ring_offsets.logical_heap_bytes_for_count(rings + 1)
                        + polygon_offsets.logical_heap_bytes_for_count(rows + 1)
                        + row_map.heap_bytes()
                },
            }
    }

    pub(crate) fn line_view<'a>(
        coords: &CoordSeq,
        offsets: &[i32],
        row_map: impl Into<RowSelectionRef<'a>>,
        logical: usize,
    ) -> CoordSeq {
        let window = row_map.into().csr_window(offsets, logical);
        coords.view(CoordWindow::from_range(window, coords.len()))
    }

    pub(crate) fn polygon_view(
        coords: &CoordSeq,
        ring_offsets: &[i32],
        rings: std::ops::Range<usize>,
    ) -> crate::error::Result<Polygon> {
        let ring = |index: usize| {
            let window = ring_offsets[index] as usize..ring_offsets[index + 1] as usize;
            Ring::from_closed(coords.view(CoordWindow::from_range(window, coords.len())))
        };
        Polygon::new(
            ring(rings.start),
            (rings.start + 1..rings.end).map(ring).collect(),
        )
    }

    pub fn point_rows(&self) -> Option<PointRows<'_>> {
        match self {
            Self::Mixed(shapes) => shapes
                .iter()
                .map(|shape| match shape {
                    Shape::Point(point) => Some(*point),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
                .map(PointRows::Gathered),
            Self::Packed(PackedStorage::Points { coords, row_map }) => Some(PointRows::Packed {
                coords,
                row_map: row_map.as_deref(),
            }),
            Self::Packed(PackedStorage::Lines { .. } | PackedStorage::Polygons { .. }) => None,
        }
    }

    pub fn line_rows(&self) -> Option<LineRows<'_>> {
        match self {
            Self::Mixed(shapes) => shapes
                .iter()
                .map(|shape| match shape {
                    Shape::LineString(seq) => Some(seq.clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
                .map(LineRows::Gathered),
            Self::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => Some(LineRows::Packed {
                coords,
                offsets: offsets.as_slice(),
                row_map: row_map.as_deref(),
            }),
            Self::Packed(PackedStorage::Points { .. } | PackedStorage::Polygons { .. }) => None,
        }
    }
    /// Combined bounds ``(minx, miny, maxx, maxy)`` over all geometries, or
    /// ``None`` if every geometry is empty.
    ///
    /// Returns
    /// -------
    /// tuple or None
    pub fn total_bounds(&self) -> Option<Bounds> {
        if let Self::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        }) = self
            && row_map.is_identity()
        {
            let mut total: Option<Bounds> = None;
            for row in 0..polygon_offsets.len().saturating_sub(1) {
                let rings = polygon_offsets[row] as usize..polygon_offsets[row + 1] as usize;
                if rings.is_empty() {
                    continue;
                }
                let vertices = ring_offsets[rings.start] as usize..ring_offsets[rings.end] as usize;
                let (minx, maxx) = crate::geometry::column_minmax(&coords.xs()[vertices.clone()])?;
                let (miny, maxy) = crate::geometry::column_minmax(&coords.ys()[vertices])?;
                let bounds = Bounds::new_raw(minx, miny, maxx, maxy);
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
        let row_box = |xs: &[f64], ys: &[f64], range: std::ops::Range<usize>| -> Result<Shape> {
            if range.is_empty() {
                // `envelope`'s output type is `Polygon` (boxes), so an empty row
                // yields `POLYGON EMPTY` — identical to scalar `Shape::envelope`.
                return Ok(Shape::empty_polygon());
            }
            let [minx, miny, maxx, maxy] = crate::geometry::xy_bounds_columns(
                column_window(xs, &range),
                column_window(ys, &range),
            );
            crate::geometry::bounds_to_shape(Bounds::new_raw(minx, miny, maxx, maxy))
        };
        match self {
            Self::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let (xs, ys) = (coords.xs(), coords.ys());
                Ok(Some(Self::try_map_polygon_rows(
                    ring_offsets,
                    polygon_offsets.as_slice(),
                    row_map.as_deref(),
                    true,
                    |window| {
                        window.map_or_else(
                            || Ok(Shape::empty_polygon()),
                            |vertices| row_box(xs, ys, vertices),
                        )
                    },
                )?))
            },
            Self::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let (xs, ys) = (coords.xs(), coords.ys());
                let values =
                    Self::try_map_line_rows(offsets.as_slice(), row_map.as_deref(), |window| {
                        row_box(xs, ys, window)
                    })?;
                Ok(Some(values))
            },
            Self::Packed(PackedStorage::Points { .. }) | Self::Mixed(_) => Ok(None),
        }
    }

    pub(crate) fn per_element_bounds(&self) -> Result<Option<Vec<Option<Bounds>>>> {
        let row_bounds =
            |xs: &[f64], ys: &[f64], range: std::ops::Range<usize>| -> Option<Bounds> {
                if range.is_empty() {
                    return None;
                }
                let (minx, maxx) = crate::geometry::column_minmax(column_window(xs, &range))?;
                let (miny, maxy) = crate::geometry::column_minmax(column_window(ys, &range))?;
                Some(Bounds::new_raw(minx, miny, maxx, maxy))
            };
        match self {
            Self::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let (xs, ys) = (coords.xs(), coords.ys());
                Self::try_map_polygon_rows(
                    ring_offsets,
                    polygon_offsets.as_slice(),
                    row_map.as_deref(),
                    true,
                    |window| Ok(window.and_then(|vertices| row_bounds(xs, ys, vertices))),
                )
                .map(Some)
            },
            Self::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let (xs, ys) = (coords.xs(), coords.ys());
                Self::try_map_line_rows(offsets.as_slice(), row_map.as_deref(), |window| {
                    Ok(row_bounds(xs, ys, window))
                })
                .map(Some)
            },
            // A point's envelope is the degenerate box at its coordinate —
            // read straight off the packed columns (no per-row scan). This
            // lets the metric broadcasts' box-separation refuter settle
            // far-apart pairs against a point array without materializing the
            // other operand. Non-finite (empty point) → `None`, so the refuter
            // defers it to the exact per-pair path.
            Self::Packed(PackedStorage::Points { coords, row_map }) => {
                let map = row_map.as_deref();
                let len = point_logical_len(coords, map);
                let mut output = Vec::new();
                crate::boundary::try_reserve_vec(&mut output, len)?;
                for logical in 0..len {
                    let point = coords.point_at(physical_row(map, logical));
                    crate::boundary::try_push_vec(
                        &mut output,
                        (point.x.is_finite() && point.y.is_finite())
                            .then_some(Bounds::from_point(point)),
                    )?;
                }
                Ok(Some(output))
            },
            // Mixed has no columnar fold; the unary bounds kernel walks
            // shapes (and the prepared-row cache must not seed `None` as
            // "known empty" when this returns None — see prepared_row).
            Self::Mixed(_) => Ok(None),
        }
    }

    pub fn iter_shapes(&self) -> ShapesIter<'_> {
        ShapesIter::new(self)
    }

    pub fn iter_rows(&self) -> RowsIter<'_> {
        RowsIter::new(self)
    }

    /// Borrow one logical row without materializing a `Shape`.
    ///
    /// This is the random-access counterpart to [`Self::iter_rows`]. Keep
    /// packed row selection and CSR-window resolution here so consumers such
    /// as the spatial index cannot grow a second interpretation of storage.
    pub(crate) fn row(&self, index: usize) -> ShapeRow<'_> {
        match self {
            Self::Mixed(shapes) => ShapeRow::Shape(&shapes[index]),
            Self::Packed(PackedStorage::Points { coords, row_map }) => {
                ShapeRow::Point(coords.point_at(physical_row(row_map.as_deref(), index)))
            },
            Self::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let window = row_map.as_deref().csr_window(offsets.as_slice(), index);
                ShapeRow::line(coords, CoordWindow::from_range(window, coords.len()))
            },
            Self::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let rings =
                    polygon_rings_range(polygon_offsets.as_slice(), row_map.as_deref(), index);
                ShapeRow::rings(coords, ring_offsets, rings)
            },
        }
    }
}

pub(crate) fn reverse_coord_windows(coords: &CoordSeq, boundaries: &[i32]) -> Result<CoordSeq> {
    let reverse_column = |column: &[f64]| -> Result<Vec<f64>> {
        let len = column.len();
        let mut output = Vec::new();
        crate::boundary::try_reserve_vec(&mut output, len)?;
        if !csr_windows_partition(boundaries, len) {
            output.extend(column.iter().rev().copied());
            return Ok(output);
        }
        for &[start, end] in boundaries.array_windows::<2>() {
            output.extend(column[start as usize..end as usize].iter().rev().copied());
        }
        Ok(output)
    };
    CoordSeq::try_from_vecs(
        reverse_column(coords.xs())?,
        reverse_column(coords.ys())?,
        coords.zs().map(reverse_column).transpose()?,
        coords.ms().map(reverse_column).transpose()?,
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
    pub(crate) fn axes(&self) -> CoordinateAxes {
        match self {
            Self::Points { coords, .. }
            | Self::Lines { coords, .. }
            | Self::Polygons { coords, .. } => coords.axes(),
        }
    }
}
