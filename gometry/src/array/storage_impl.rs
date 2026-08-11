#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use crate::array::{
    Bounds, CoordSeq, CoordinateAxes, Frame, GeometryArrayStorage, LineRows, PointRows, Polygon,
    PyGeometry, Ring, RowSelectionRef, RowsIter, Shape, ShapeData, ShapeRow, ShapesIter,
    column_window, line_logical_len, packed_lines_coord_len, packed_polygons_coord_len,
    packed_polygons_ring_len, physical_row, point_logical_len, polygon_logical_len,
    polygon_rings_range,
};
use crate::geometry::{CoordWindow, LineSeq};

const fn coordseq_logical_bytes(coords: &CoordSeq, count: usize) -> usize {
    coords.axes().byte_width(count)
}

impl GeometryArrayStorage {
    /// The one coordinate layout carried by homogeneous packed storage.
    /// Selections only remap rows and missing slots use placeholders with the
    /// same columns, so this is O(1) for points, lines, and polygons.
    pub(crate) fn packed_axes(&self) -> Option<CoordinateAxes> {
        match self {
            Self::Points { coords, .. }
            | Self::Lines { coords, .. }
            | Self::Polygons { coords, .. } => Some(coords.axes()),
            Self::Mixed(_) => None,
        }
    }

    /// Declared kind for homogeneous packed storage (`Point` / `LineString` /
    /// `Polygon`). `Mixed` (and Multi* rows that live there) returns `None`.
    pub(crate) const fn homogeneous_kind(&self) -> Option<crate::GeometryKind> {
        match self {
            Self::Points { .. } => Some(crate::GeometryKind::Point),
            Self::Lines { .. } => Some(crate::GeometryKind::LineString),
            Self::Polygons { .. } => Some(crate::GeometryKind::Polygon),
            Self::Mixed(_) => None,
        }
    }

    pub(crate) fn const_or_shape_bool(
        &self,
        constant: bool,
        shape_pred: impl Fn(&Shape) -> bool,
    ) -> Vec<bool> {
        match self {
            Self::Points { .. } | Self::Polygons { .. } => {
                std::iter::repeat_n(constant, self.len()).collect()
            },
            _ => self.iter_shapes().map(|shape| shape_pred(&shape)).collect(),
        }
    }

    pub(crate) fn lines_bool(&self, line: impl Fn(&CoordSeq) -> bool) -> Option<Vec<bool>> {
        match self {
            Self::Lines {
                coords,
                offsets,
                row_map,
            } => {
                let map = row_map.as_deref();
                Some(
                    (0..line_logical_len(offsets.as_slice(), map))
                        .map(|logical| {
                            let window = map.csr_window(offsets.as_slice(), logical);
                            line(&coords.view(CoordWindow::trusted(window, coords.len())))
                        })
                        .collect(),
                )
            },
            _ => None,
        }
    }

    /// Dense polygon-row bool kernel (no missing-mask branch).
    pub(crate) fn polygons_bool(&self, polygon: impl Fn(&Polygon) -> bool) -> Option<Vec<bool>> {
        match self {
            Self::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => {
                let map = row_map.as_deref();
                Some(
                    (0..polygon_logical_len(polygon_offsets.as_slice(), map))
                        .map(|logical| {
                            polygon(&Self::polygon_view(
                                coords,
                                ring_offsets,
                                polygon_rings_range(polygon_offsets.as_slice(), map, logical),
                            ))
                        })
                        .collect(),
                )
            },
            _ => None,
        }
    }

    /// Like [`lines_bool`], but writes `false` for missing rows without
    /// evaluating the kernel on NaN placeholders. Call only when a missing
    /// mask is present — dense input uses [`lines_bool`].
    pub(crate) fn lines_bool_masked(
        &self,
        missing: Option<&crate::array::MissingMask>,
        line: impl Fn(&CoordSeq) -> bool,
    ) -> Option<Vec<bool>> {
        let Some(mask) = missing else {
            return self.lines_bool(line);
        };
        match self {
            Self::Lines {
                coords,
                offsets,
                row_map,
            } => {
                let map = row_map.as_deref();
                Some(
                    (0..line_logical_len(offsets.as_slice(), map))
                        .map(|logical| {
                            if mask.is_missing(logical) {
                                return false;
                            }
                            let window = map.csr_window(offsets.as_slice(), logical);
                            line(&coords.view(CoordWindow::trusted(window, coords.len())))
                        })
                        .collect(),
                )
            },
            _ => None,
        }
    }

    /// Polygon-row bool kernel; writes `false` for missing rows without
    /// evaluating on NaN placeholders. Call only when a missing mask is
    /// present — dense input uses [`polygons_bool`].
    pub(crate) fn polygons_bool_masked(
        &self,
        missing: Option<&crate::array::MissingMask>,
        polygon: impl Fn(&Polygon) -> bool,
    ) -> Option<Vec<bool>> {
        let Some(mask) = missing else {
            return self.polygons_bool(polygon);
        };
        match self {
            Self::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => {
                let map = row_map.as_deref();
                Some(
                    (0..polygon_logical_len(polygon_offsets.as_slice(), map))
                        .map(|logical| {
                            if mask.is_missing(logical) {
                                return false;
                            }
                            polygon(&Self::polygon_view(
                                coords,
                                ring_offsets,
                                polygon_rings_range(polygon_offsets.as_slice(), map, logical),
                            ))
                        })
                        .collect(),
                )
            },
            _ => None,
        }
    }

    pub(crate) fn map_line_rows<'a, T>(
        offsets: &[i32],
        row_map: impl Into<RowSelectionRef<'a>>,
        f: impl Fn(std::ops::Range<usize>) -> T,
    ) -> Vec<T> {
        let map = row_map.into();
        (0..line_logical_len(offsets, map))
            .map(|logical| f(map.csr_window(offsets, logical)))
            .collect()
    }

    pub(crate) fn map_polygon_rows<'a, T>(
        ring_offsets: &[i32],
        polygon_offsets: &[i32],
        row_map: impl Into<RowSelectionRef<'a>>,
        all_rings: bool,
        f: impl Fn(Option<std::ops::Range<usize>>) -> T,
    ) -> Vec<T> {
        let map = row_map.into();
        (0..polygon_logical_len(polygon_offsets, map))
            .map(|logical| {
                let rings = polygon_rings_range(polygon_offsets, map, logical);
                let window = if rings.is_empty() {
                    None
                } else if all_rings {
                    Some(ring_offsets[rings.start] as usize..ring_offsets[rings.end] as usize)
                } else {
                    Some(ring_offsets[rings.start] as usize..ring_offsets[rings.start + 1] as usize)
                };
                f(window)
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Mixed(items) => items.len(),
            Self::Points { coords, row_map } => point_logical_len(coords, row_map.as_deref()),
            Self::Lines {
                offsets, row_map, ..
            } => line_logical_len(offsets.as_slice(), row_map.as_deref()),
            Self::Polygons {
                polygon_offsets,
                row_map,
                ..
            } => polygon_logical_len(polygon_offsets.as_slice(), row_map.as_deref()),
        }
    }

    pub(crate) fn logical_coordinate_bytes(&self) -> usize {
        match self {
            Self::Mixed(shapes) => shapes.iter().map(Shape::coordinate_bytes).sum(),
            Self::Points { coords, row_map } => {
                coordseq_logical_bytes(coords, point_logical_len(coords, row_map.as_deref()))
            },
            Self::Lines {
                coords,
                offsets,
                row_map,
            } => coordseq_logical_bytes(
                coords,
                packed_lines_coord_len(offsets.as_slice(), row_map.as_deref()),
            ),
            Self::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => coordseq_logical_bytes(
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
        match self {
            // Shape payload + the Vec's own allocation. Prepared ShapeData
            // (array-side cache) is counted on PyGeometryArray::heap_bytes
            // once slots initialize — see prepared_cache_heap_bytes.
            Self::Mixed(shapes) => {
                self.logical_coordinate_bytes() + shapes.len() * std::mem::size_of::<Shape>()
            },
            Self::Points { row_map, .. } => self.logical_coordinate_bytes() + row_map.heap_bytes(),
            Self::Lines {
                offsets, row_map, ..
            } => {
                let rows = line_logical_len(offsets.as_slice(), row_map.as_deref());
                self.logical_coordinate_bytes()
                    + (rows + 1) * std::mem::size_of::<i32>()
                    + row_map.heap_bytes()
            },
            Self::Polygons {
                polygon_offsets,
                row_map,
                ..
            } => {
                let rows = polygon_logical_len(polygon_offsets.as_slice(), row_map.as_deref());
                let rings =
                    packed_polygons_ring_len(polygon_offsets.as_slice(), row_map.as_deref());
                self.logical_coordinate_bytes()
                    + (rings + 1) * std::mem::size_of::<i32>()
                    + (rows + 1) * std::mem::size_of::<i32>()
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
        coords.view(CoordWindow::trusted(window, coords.len()))
    }

    pub(crate) fn polygon_view(
        coords: &CoordSeq,
        ring_offsets: &[i32],
        rings: std::ops::Range<usize>,
    ) -> Polygon {
        let ring = |index: usize| {
            let window = ring_offsets[index] as usize..ring_offsets[index + 1] as usize;
            Ring::from_trusted_closed(coords.view(CoordWindow::trusted(window, coords.len())))
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
            Self::Points { coords, row_map } => Some(PointRows::Packed {
                coords,
                row_map: row_map.as_deref(),
            }),
            Self::Lines { .. } | Self::Polygons { .. } => None,
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
            Self::Lines {
                coords,
                offsets,
                row_map,
            } => Some(LineRows::Packed {
                coords,
                offsets: offsets.as_slice(),
                row_map: row_map.as_deref(),
            }),
            Self::Points { .. } | Self::Polygons { .. } => None,
        }
    }
    /// Combined bounds ``(minx, miny, maxx, maxy)`` over all geometries, or
    /// ``None`` if every geometry is empty.
    ///
    /// Returns
    /// -------
    /// tuple or None
    pub fn total_bounds(&self) -> Option<Bounds> {
        // Points and lines use every stored ordinate. Polygon envelopes use
        // only shells: external holes are invalid but still representable, and
        // must not expand an array bound beyond the scalar polygon contract.
        if let Self::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } = self
            && row_map.is_identity()
        {
            let mut total: Option<Bounds> = None;
            for row in 0..polygon_offsets.len().saturating_sub(1) {
                let rings = polygon_offsets[row] as usize..polygon_offsets[row + 1] as usize;
                let Some(shell_index) = (!rings.is_empty()).then_some(rings.start) else {
                    continue;
                };
                let shell =
                    ring_offsets[shell_index] as usize..ring_offsets[shell_index + 1] as usize;
                let (minx, maxx) = crate::geometry::column_minmax(&coords.xs()[shell.clone()])?;
                let (miny, maxy) = crate::geometry::column_minmax(&coords.ys()[shell])?;
                let bounds = Bounds::new_unchecked(minx, miny, maxx, maxy);
                match &mut total {
                    Some(current) => current.include_bounds(bounds),
                    None => total = Some(bounds),
                }
            }
            return total;
        }
        let packed = match self {
            Self::Points { coords, row_map }
            | Self::Lines {
                coords, row_map, ..
            } => row_map.is_identity().then_some(coords),
            Self::Polygons { .. } | Self::Mixed(_) => None,
        };
        if let Some(seq) = packed {
            let (minx, maxx) = crate::geometry::column_minmax(seq.xs())?;
            let (miny, maxy) = crate::geometry::column_minmax(seq.ys())?;
            return Some(Bounds::new_unchecked(minx, miny, maxx, maxy));
        }
        // Mixed rows fold their cached per-row bounds.
        let mut values = self.iter_shapes().filter_map(|shape| shape.bounds());
        let first = values.next()?;
        Some(values.fold(first, |mut acc, bounds| {
            acc.include_bounds(bounds);
            acc
        }))
    }

    pub(crate) fn envelope_box_shapes(&self) -> Option<Vec<Shape>> {
        let row_box = |xs: &[f64], ys: &[f64], range: std::ops::Range<usize>| -> Shape {
            if range.is_empty() {
                // `envelope`'s output type is `Polygon` (boxes), so an empty row
                // yields `POLYGON EMPTY` — identical to scalar `Shape::envelope`.
                return Shape::empty_polygon();
            }
            let [minx, miny, maxx, maxy] = crate::geometry::xy_bounds_columns(
                column_window(xs, &range),
                column_window(ys, &range),
            );
            crate::geometry::bounds_to_shape(Bounds::new_unchecked(minx, miny, maxx, maxy))
        };
        match self {
            Self::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => {
                let (xs, ys) = (coords.xs(), coords.ys());
                Some(Self::map_polygon_rows(
                    ring_offsets,
                    polygon_offsets.as_slice(),
                    row_map.as_deref(),
                    false,
                    |window| {
                        window.map_or_else(Shape::empty_polygon, |shell| row_box(xs, ys, shell))
                    },
                ))
            },
            Self::Lines {
                coords,
                offsets,
                row_map,
            } => {
                let (xs, ys) = (coords.xs(), coords.ys());
                Some(Self::map_line_rows(
                    offsets.as_slice(),
                    row_map.as_deref(),
                    |window| row_box(xs, ys, window),
                ))
            },
            Self::Points { .. } | Self::Mixed(_) => None,
        }
    }

    pub(crate) fn per_element_bounds(&self) -> Option<Vec<Option<Bounds>>> {
        let row_bounds =
            |xs: &[f64], ys: &[f64], range: std::ops::Range<usize>| -> Option<Bounds> {
                if range.is_empty() {
                    return None;
                }
                let (minx, maxx) = crate::geometry::column_minmax(column_window(xs, &range))?;
                let (miny, maxy) = crate::geometry::column_minmax(column_window(ys, &range))?;
                Some(Bounds::new_unchecked(minx, miny, maxx, maxy))
            };
        match self {
            Self::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => {
                let (xs, ys) = (coords.xs(), coords.ys());
                Some(Self::map_polygon_rows(
                    ring_offsets,
                    polygon_offsets.as_slice(),
                    row_map.as_deref(),
                    false,
                    |window| window.and_then(|shell| row_bounds(xs, ys, shell)),
                ))
            },
            Self::Lines {
                coords,
                offsets,
                row_map,
            } => {
                let (xs, ys) = (coords.xs(), coords.ys());
                Some(Self::map_line_rows(
                    offsets.as_slice(),
                    row_map.as_deref(),
                    |window| row_bounds(xs, ys, window),
                ))
            },
            // A point's envelope is the degenerate box at its coordinate —
            // read straight off the packed columns (no per-row scan). This
            // lets the metric broadcasts' box-separation refuter settle
            // far-apart pairs against a point array without materializing the
            // other operand. Non-finite (empty point) → `None`, so the refuter
            // defers it to the exact per-pair path.
            Self::Points { coords, row_map } => {
                let map = row_map.as_deref();
                Some(
                    (0..point_logical_len(coords, map))
                        .map(|logical| {
                            let point = coords.point_at(physical_row(map, logical));
                            (point.x.is_finite() && point.y.is_finite())
                                .then_some(Bounds::from_point(point))
                        })
                        .collect(),
                )
            },
            // Mixed has no columnar fold; the unary bounds kernel walks
            // shapes (and the prepared-row cache must not seed `None` as
            // "known empty" when this returns None — see with_row_data).
            Self::Mixed(_) => None,
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
            Self::Points { coords, row_map } => {
                ShapeRow::Point(coords.point_at(physical_row(row_map.as_deref(), index)))
            },
            Self::Lines {
                coords,
                offsets,
                row_map,
            } => {
                let window = row_map.as_deref().csr_window(offsets.as_slice(), index);
                ShapeRow::Line(coords, window.start, window.end)
            },
            Self::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => {
                let rings =
                    polygon_rings_range(polygon_offsets.as_slice(), row_map.as_deref(), index);
                ShapeRow::Rings(coords, ring_offsets, rings.start, rings.end)
            },
        }
    }

    /// Build a scalar geometry for a packed row (always fresh ShapeData).
    ///
    /// Mixed rows must go through [`PyGeometryArray::geometry_at`] so the
    /// array-side prepared cache owns the ShapeData.
    pub(crate) fn geometry_at(
        &self,
        index: usize,
        frame: Frame,
        frame_cache: Arc<crate::geometry::FrameDependentCaches>,
    ) -> PyGeometry {
        match self {
            Self::Mixed(shapes) => PyGeometry {
                shape: Arc::new(ShapeData::new(shapes[index].clone())),
                frame_cache,
                frame,
            },
            Self::Points { coords, row_map } => PyGeometry {
                shape: Arc::new(ShapeData::new(Shape::Point(
                    coords.point_at(physical_row(row_map.as_deref(), index)),
                ))),
                frame_cache,
                frame,
            },
            Self::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            } => PyGeometry {
                shape: Arc::new(ShapeData::new(Shape::Polygon(Self::polygon_view(
                    coords,
                    ring_offsets,
                    polygon_rings_range(polygon_offsets.as_slice(), row_map.as_deref(), index),
                )))),
                frame_cache,
                frame,
            },
            Self::Lines {
                coords,
                offsets,
                row_map,
            } => PyGeometry {
                shape: Arc::new(ShapeData::new(Shape::LineString(LineSeq::from_trusted(
                    Self::line_view(coords, offsets, row_map.as_deref(), index),
                )))),
                frame_cache,
                frame,
            },
        }
    }
}

pub(crate) fn reverse_coord_windows(coords: &CoordSeq, boundaries: &[i32]) -> CoordSeq {
    // Exact-final-Arc reverse of each CSR window (no `Vec`/`Box` intermediate).
    // Completeness is enforced by construction: windows must partition
    // `0..len` contiguously before any `assume_init`. A malformed offset
    // table falls back to a whole-column reverse (still memory-safe).
    let reverse_column = |column: &[f64]| -> std::sync::Arc<[f64]> {
        let len = column.len();
        if !csr_windows_partition(boundaries, len) {
            return crate::geometry::reverse_column_arc(column);
        }
        let mut buf = std::sync::Arc::<[f64]>::new_uninit_slice(len);
        // SAFETY: unique Arc; the partition check above proves every index
        // `0..len` is written exactly once by the window loop (no OOB, no
        // gaps), so `assume_init` is sound.
        unsafe {
            let dst = std::sync::Arc::get_mut(&mut buf)
                .unwrap_unchecked()
                .as_mut_ptr()
                .cast::<f64>();
            let mut out = 0_usize;
            for &[start, end] in boundaries.array_windows::<2>() {
                let (start, end) = (start as usize, end as usize);
                let n = end - start;
                for i in 0..n {
                    dst.add(out + i).write(column[end - 1 - i]);
                }
                out += n;
            }
            debug_assert_eq!(out, len);
            buf.assume_init()
        }
    };
    CoordSeq::from_columns(
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
