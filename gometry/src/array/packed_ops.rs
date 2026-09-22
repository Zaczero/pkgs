use std::sync::Arc;

use crate::HeapSize as _;
use crate::array::{
    ArrayRow, Bound, CoordSeq, GeometryArrayStorage, PackedColumnBuilder, PackedStorage, Point,
    PyAny, PyGeometry, PyGeometryArray, PyResult, Python, Result, RowSelectionRef, Shape,
    column_window, curves, line_logical_len, note_array_row, parse_curve_bounds, physical_row,
    rows_err,
};
use crate::boundary::input;
use crate::py::vectors::Groups;

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
    /// Coordinate payload for a contiguous logical row range without building
    /// a selected `GeometryArray` or composing a row map.
    pub(crate) fn logical_coordinate_bytes_range(&self, rows: std::ops::Range<usize>) -> usize {
        debug_assert!(rows.end <= self.len());
        match self.storage() {
            GeometryArrayStorage::Mixed(shapes) => rows
                .filter(|&logical| !self.is_row_missing(logical))
                .map(|logical| shapes[logical].coordinate_bytes())
                .sum(),
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) => {
                let count = if self.is_compact_nullable() {
                    if let Some(missing) = self.missing() {
                        rows.filter(|&logical| missing.is_valid(logical)).count()
                    } else {
                        rows.len()
                    }
                } else {
                    rows.len()
                };
                coords.axes().byte_width(count)
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
        let owner =
            crate::heap_size::ARC_CONTROL_BYTES + std::mem::size_of::<GeometryArrayStorage>();
        owner
            + match self.storage() {
                GeometryArrayStorage::Mixed(shapes) => {
                    // Keep the range's visible coordinate payload separate
                    // from nested mixed-shape owners.
                    let selected_shapes = &shapes[rows.clone()];
                    coordinate_bytes
                        + rows.len() * std::mem::size_of::<Shape>()
                        + selected_shapes
                            .iter()
                            .zip(rows)
                            .filter(|(_, logical)| !self.is_row_missing(*logical))
                            .map(|(shape, _)| {
                                shape.heap_bytes().saturating_sub(shape.coordinate_bytes())
                            })
                            .sum::<usize>()
                },
                GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                    let count = if self.is_compact_nullable() {
                        self.missing().map_or_else(
                            || rows.len(),
                            |missing| {
                                rows.clone()
                                    .filter(|&logical| missing.is_valid(logical))
                                    .count()
                            },
                        )
                    } else {
                        rows.len()
                    };
                    crate::heap_size::ARC_CONTROL_BYTES
                        + std::mem::size_of::<CoordSeq>()
                        + coords.logical_heap_bytes_for_count(count)
                        + selected_row_map_heap_bytes(row_map.as_deref(), rows)
                },
                GeometryArrayStorage::Packed(PackedStorage::Lines {
                    coords,
                    offsets,
                    row_map,
                }) => {
                    let map = row_map.as_deref();
                    let vertices: usize = rows
                        .clone()
                        .map(|logical| map.csr_window(offsets, logical).len())
                        .sum();
                    crate::heap_size::ARC_CONTROL_BYTES
                        + std::mem::size_of::<CoordSeq>()
                        + coords.logical_heap_bytes_for_count(vertices)
                        + offsets.logical_heap_bytes_for_count(rows.len() + 1)
                        + selected_row_map_heap_bytes(row_map.as_deref(), rows)
                },
                GeometryArrayStorage::Packed(PackedStorage::Polygons {
                    coords,
                    ring_offsets,
                    polygon_offsets,
                    row_map,
                }) => {
                    let map = row_map.as_deref();
                    let rings: usize = rows
                        .clone()
                        .map(|logical| map.csr_window(polygon_offsets, logical).len())
                        .sum();
                    let vertices: usize = rows
                        .clone()
                        .map(|logical| {
                            let rings = map.csr_window(polygon_offsets, logical);
                            if rings.is_empty() {
                                0
                            } else {
                                ring_offsets[rings.end] as usize
                                    - ring_offsets[rings.start] as usize
                            }
                        })
                        .sum();
                    crate::heap_size::ARC_CONTROL_BYTES
                        + std::mem::size_of::<CoordSeq>()
                        + coords.logical_heap_bytes_for_count(vertices)
                        + ring_offsets.logical_heap_bytes_for_count(rings + 1)
                        + polygon_offsets.logical_heap_bytes_for_count(rows.len() + 1)
                        + selected_row_map_heap_bytes(map, rows)
                },
            }
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
        let mut builder = PackedColumnBuilder::like(coords, 0);
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
        )?)
    }

    pub(crate) fn simplify_vw_packed_lines(
        &self,
        coords: &CoordSeq,
        offsets: &[i32],
        tolerance: f64,
    ) -> PyResult<Self> {
        self.simplify_packed_lines_mask(coords, offsets, |xs, ys, keep| {
            // Distance-scale threshold; area form is derived after framing.
            Ok(crate::geometry::vw_keep(xs, ys, tolerance, keep))
        })
        .map_err(crate::PyErr::from)
    }

    pub(crate) fn simplify_dp_packed_lines(
        &self,
        coords: &CoordSeq,
        offsets: &[i32],
        tolerance: f64,
    ) -> PyResult<Self> {
        // One batch-local DP work stack reused across rows (clear+push, no
        // per-row allocation). Free-threading: not a receiver cache / lock.
        let result = {
            let mut stack: Vec<(usize, usize)> = Vec::new();
            self.simplify_packed_lines_mask(coords, offsets, |xs, ys, keep| {
                crate::geometry::rdp_keep(xs, ys, tolerance, keep, &mut stack)
            })
        };
        result.map_err(crate::PyErr::from)
    }

    /// Douglas-Peucker / VW over packed lines, CSR to CSR: one keep-mask per
    /// row window, kept vertices appended into one new column set.
    pub(crate) fn simplify_packed_lines_mask(
        &self,
        coords: &CoordSeq,
        offsets: &[i32],
        mut keep_mask: impl FnMut(&[f64], &[f64], &mut Vec<bool>) -> crate::error::Result<Option<usize>>,
    ) -> Result<Self> {
        let (xs, ys) = (coords.xs(), coords.ys());
        let rows = offsets.len() - 1;
        let mut builder = PackedColumnBuilder::like(coords, xs.len());
        let mut keep: Vec<bool> = Vec::new();
        for row in 0..rows {
            let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
            let window = start..end;
            let kept = keep_mask(
                column_window(xs, &window),
                column_window(ys, &window),
                &mut keep,
            )?;
            // Too short or nothing removable: the row passes through whole.
            if kept.is_none_or(|count| count == end - start) {
                builder.push_window(coords, window)?;
                continue;
            }
            for (slot, &kept) in keep.iter().enumerate() {
                if kept {
                    builder.push_vertex(coords, start + slot);
                }
            }
            builder.close_row()?;
        }
        let cap = builder.vertex_len();
        let (out_coords, out_offsets) = builder.finish(cap)?;
        Self::packed_lines(out_coords, out_offsets, self.frame.clone())
    }

    /// Shared curve-key engine: one frame for the whole array (explicit)
    /// bounds or total bounds), one key per row. Empty and missing rows use
    /// `u64::MAX`, the documented sort-last sentinel.
    pub(crate) fn curve_keys(
        &self,
        level: i64,
        bounds: Option<&Bound<'_, PyAny>>,
        kind: curves::CurveKind,
    ) -> PyResult<Vec<u64>> {
        let level = input::validate_curve_level(level)?;
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
        let mut order: Vec<_> = (0..keys.len()).collect();
        order.sort_unstable_by_key(|&row| (self.is_row_missing(row), keys[row], row));
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
    ) -> PyResult<Groups> {
        self.flat_map_shapes_groups_indexed(py, move |_, shape| transform(shape))
    }

    pub(crate) fn flat_map_shapes_groups_indexed(
        &self,
        py: Python<'_>,
        transform: impl Fn(usize, &Shape) -> Result<Vec<Shape>> + Send + Sync,
    ) -> PyResult<Groups> {
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
        Groups::from_geometry_builder(frame, builder)
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
    ) -> PyResult<Groups> {
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
                            let mut budget =
                                crate::geometry::ExpansionBudget::new(operation, parameter);
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
        Groups::from_geometry_builder(frame, builder)
    }

    /// Packed triangle analogue of [`flat_map_shapes_groups_budgeted`]: the
    /// triangle vertex emitter charges each row's budget before its columns
    /// are appended.
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
    ) -> PyResult<Groups> {
        let array = self.clone();
        let frame = self.frame.clone();
        let builder = py
            .detach(move || {
                let mut builder = crate::CsrBuilder::<Point>::new();
                for logical_row in array.row_operation().rows_with_missing(Vec::new) {
                    let (row, values) = match logical_row {
                        ArrayRow::Missing { logical, value } => (logical, value),
                        ArrayRow::Present(row) => {
                            let logical = row.logical();
                            let mut budget =
                                crate::geometry::ExpansionBudget::new(operation, parameter);
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
                        builder.values_mut().push(value);
                    }
                    let end = builder.len() / 4;
                    builder.push_end(end).map_err(|error| (row, error))?;
                }
                Ok::<_, (usize, crate::error::Error)>(builder)
            })
            .map_err(rows_err)?;
        Groups::from_point_builder(frame, builder)
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
            )?
            .with_missing_mask(self.missing().cloned())?);
        }
        self.map_shapes(|shape| Ok(shape.boundary()?))
    }

    /// A packed point array's rows as ONE `Shape::MultiPoint` over the shared
    /// coordinate columns — the zero-copy staging for whole-array unions
    /// (`union_all()`): the same kernel and output as N point rows, with no
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
            Shape::MultiPoint(crate::geometry::MultiPoint::from_coordseq(
                Self::gather_point_coords(coords, row_map.as_deref())?,
            )?)
        }))
    }
}

#[path = "map_shapes.rs"]
mod map_shapes;
