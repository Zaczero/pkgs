use std::sync::Arc;

use pyo3::PyErr;

use crate::array::{
    ArrayRow, Bound, CoordSeq, CsrOffsetColumn, Point, PyAny, PyGeometry, PyGeometryArray,
    PyResult, Python, Result, Shape, curves, note_array_row, parse_curve_bounds, rows_err,
};
use crate::boundary::input;
use crate::geographic::PeriodicAxis;
use crate::geometry::storage::builder::PackedColumnBuilder;
use crate::geometry::storage::gather::gather_line_columns;
#[cfg(test)]
use crate::geometry::storage::record_csr_size_row_visit;
use crate::geometry::storage::{
    GeometryArrayStorage, PackedRow, PackedStorage, RowSelectionRef, column_window,
    csr_value_range, csr_value_window, line_logical_len, physical_row,
};
use crate::geometry::types::ShapeHeapMemo;
use crate::py::vectors::Groups;

fn selected_row_map_heap_bytes(
    row_map: RowSelectionRef<'_>,
    rows: std::ops::Range<usize>,
) -> usize {
    match row_map {
        RowSelectionRef::Identity | RowSelectionRef::Window { .. } => 0,
        RowSelectionRef::Gather(_) => {
            let Some(first_logical) = rows.clone().next() else {
                return 0;
            };
            let first_physical = physical_row(row_map, first_logical);
            if rows.clone().enumerate().all(|(offset, logical)| {
                first_physical.checked_add(offset) == Some(physical_row(row_map, logical))
            }) {
                0
            } else {
                rows.len().saturating_mul(std::mem::size_of::<usize>())
            }
        },
    }
}

impl PyGeometryArray {
    /// Logical heap footprint for a contiguous logical row range without
    /// materializing its row-selection metadata.
    pub(crate) fn logical_heap_bytes_range(&self, rows: std::ops::Range<usize>) -> usize {
        let owner = crate::heap_size::ARC_CONTROL_BYTES
            .saturating_add(std::mem::size_of::<GeometryArrayStorage>());
        owner.saturating_add(match self.storage() {
            GeometryArrayStorage::Mixed(shapes) => {
                let mut memo = ShapeHeapMemo::default();
                let selected_shapes = shapes
                    .get(rows.clone())
                    .expect("logical row range is bounded by mixed storage");
                selected_shapes
                    .iter()
                    .zip(rows.clone())
                    .filter(|(_, logical)| !self.is_row_missing(*logical))
                    .fold(
                        rows.len().saturating_mul(std::mem::size_of::<Shape>()),
                        |bytes, (shape, _)| bytes.saturating_add(memo.heap_bytes(shape)),
                    )
            },
            GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) => {
                let count = if self.is_compact_nullable() {
                    self.missing().map_or_else(
                        || rows.len(),
                        |missing| missing.present_in_range(rows.clone()),
                    )
                } else {
                    rows.len()
                };
                crate::heap_size::ARC_CONTROL_BYTES
                    .saturating_add(std::mem::size_of::<CoordSeq>())
                    .saturating_add(coords.logical_heap_bytes_for_count_saturating(count))
                    .saturating_add(selected_row_map_heap_bytes(row_map.as_deref(), rows))
            },
            GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                let vertices = csr_value_range(offsets.as_slice(), map, rows.clone()).map_or_else(
                    || {
                        rows.clone()
                            .map(|logical| {
                                #[cfg(test)]
                                record_csr_size_row_visit();
                                map.csr_window(offsets, logical).len()
                            })
                            .fold(0, usize::saturating_add)
                    },
                    |vertices| vertices.len(),
                );
                crate::heap_size::ARC_CONTROL_BYTES
                    .saturating_add(std::mem::size_of::<CoordSeq>())
                    .saturating_add(coords.logical_heap_bytes_for_count_saturating(vertices))
                    .saturating_add(
                        offsets.logical_heap_bytes_for_count(rows.len().saturating_add(1)),
                    )
                    .saturating_add(selected_row_map_heap_bytes(row_map.as_deref(), rows))
            },
            GeometryArrayStorage::Packed(PackedStorage::Polygons {
                coords,
                ring_offsets,
                polygon_offsets,
                row_map,
            }) => {
                let map = row_map.as_deref();
                let (rings, vertices) =
                    csr_value_range(polygon_offsets.as_slice(), map, rows.clone()).map_or_else(
                        || {
                            let rings = rows
                                .clone()
                                .map(|logical| {
                                    #[cfg(test)]
                                    record_csr_size_row_visit();
                                    map.csr_window(polygon_offsets.as_slice(), logical).len()
                                })
                                .fold(0_usize, usize::saturating_add);
                            let vertices = rows
                                .clone()
                                .map(|logical| {
                                    #[cfg(test)]
                                    record_csr_size_row_visit();
                                    let rings = map.csr_window(polygon_offsets.as_slice(), logical);
                                    if rings.is_empty() {
                                        0
                                    } else {
                                        csr_value_window(ring_offsets.as_slice(), rings).len()
                                    }
                                })
                                .fold(0_usize, usize::saturating_add);
                            (rings, vertices)
                        },
                        |ring_range| {
                            let vertices = if ring_range.is_empty() {
                                0
                            } else {
                                csr_value_range(
                                    ring_offsets.as_slice(),
                                    RowSelectionRef::Identity,
                                    ring_range.clone(),
                                )
                                .expect("admitted ring offsets include selected endpoints")
                                .len()
                            };
                            (ring_range.len(), vertices)
                        },
                    );
                crate::heap_size::ARC_CONTROL_BYTES
                    .saturating_add(std::mem::size_of::<CoordSeq>())
                    .saturating_add(coords.logical_heap_bytes_for_count_saturating(vertices))
                    .saturating_add(
                        ring_offsets.logical_heap_bytes_for_count(rings.saturating_add(1)),
                    )
                    .saturating_add(
                        polygon_offsets.logical_heap_bytes_for_count(rows.len().saturating_add(1)),
                    )
                    .saturating_add(selected_row_map_heap_bytes(map, rows))
            },
        })
    }

    /// Gather logical packed-line rows into fresh CSR columns in logical order,
    /// resolving each through `row_map` when present.
    pub(crate) fn materialize_packed_lines_parts<'a>(
        &self,
        coords: &CoordSeq,
        offsets: &[i32],
        row_map: impl Into<RowSelectionRef<'a>>,
    ) -> PyResult<Self> {
        let (out_coords, out_offsets) = gather_line_columns(coords, offsets, row_map.into())?;
        Ok(Self::packed_lines(
            out_coords,
            out_offsets,
            self.frame.clone(),
        )?)
    }

    pub(crate) fn simplify_vw_packed_lines(
        &self,
        coords: &CoordSeq,
        offsets: &CsrOffsetColumn,
        tolerance: f64,
    ) -> PyResult<Self> {
        self.simplify_packed_lines_mask(coords, offsets, |xs, ys, keep| {
            // Distance-scale threshold; area form is derived after framing.
            Ok(crate::geometry::vw_keep(xs, ys, tolerance, keep))
        })
        .map_err(crate::PyErr::from)
    }

    pub(crate) fn simplify_vw_packed_lines_geographic(
        &self,
        coords: &CoordSeq,
        offsets: &CsrOffsetColumn,
        tolerance: f64,
        axis: PeriodicAxis,
    ) -> PyResult<Self> {
        self.simplify_packed_lines_mask(coords, offsets, |xs, ys, keep| {
            Ok(crate::geometry::vw_keep_geographic(
                xs, ys, tolerance, axis, keep,
            ))
        })
        .map_err(crate::PyErr::from)
    }

    pub(crate) fn simplify_dp_packed_lines(
        &self,
        coords: &CoordSeq,
        offsets: &CsrOffsetColumn,
        tolerance: f64,
    ) -> PyResult<Self> {
        self.simplify_dp_packed_lines_with_guard::<crate::geometry::PlanarRdpGuard>(
            coords,
            offsets,
            tolerance,
            crate::geometry::PlanarRdpGuard,
        )
    }

    pub(crate) fn simplify_dp_packed_lines_geographic(
        &self,
        coords: &CoordSeq,
        offsets: &CsrOffsetColumn,
        tolerance: f64,
        axis: PeriodicAxis,
    ) -> PyResult<Self> {
        self.simplify_dp_packed_lines_with_guard(
            coords,
            offsets,
            tolerance,
            crate::geometry::GeographicRdpGuard(&axis),
        )
    }

    pub(crate) fn simplify_packed_lines_with_method(
        &self,
        coords: &CoordSeq,
        offsets: &CsrOffsetColumn,
        tolerance: f64,
        method: crate::geometry::SimplifyMethod,
        longitude_axis: Option<PeriodicAxis>,
    ) -> PyResult<Self> {
        match (method, longitude_axis) {
            (crate::geometry::SimplifyMethod::Vw, Some(axis)) => {
                self.simplify_vw_packed_lines_geographic(coords, offsets, tolerance, axis)
            },
            (crate::geometry::SimplifyMethod::Vw, None) => {
                self.simplify_vw_packed_lines(coords, offsets, tolerance)
            },
            (crate::geometry::SimplifyMethod::Dp, Some(axis)) => {
                self.simplify_dp_packed_lines_geographic(coords, offsets, tolerance, axis)
            },
            (crate::geometry::SimplifyMethod::Dp, None) => {
                self.simplify_dp_packed_lines(coords, offsets, tolerance)
            },
        }
    }

    fn simplify_dp_packed_lines_with_guard<G: crate::geometry::RdpEdgeSpanGuard>(
        &self,
        coords: &CoordSeq,
        offsets: &CsrOffsetColumn,
        tolerance: f64,
        edge_guard: G,
    ) -> PyResult<Self> {
        // One batch-local DP work stack reused across rows (clear+push, no
        // per-row allocation). Free-threading: not a receiver cache / lock.
        let result = {
            let mut stack: Vec<(usize, usize)> = Vec::new();
            self.simplify_packed_lines_mask(coords, offsets, |xs, ys, keep| {
                crate::geometry::rdp_keep_with_edge_guard(
                    xs, ys, tolerance, edge_guard, keep, &mut stack,
                )
            })
        };
        result.map_err(crate::PyErr::from)
    }

    /// Douglas-Peucker / VW over packed lines, CSR to CSR: one keep-mask per
    /// row window, kept vertices appended into one new column set.
    pub(crate) fn simplify_packed_lines_mask(
        &self,
        coords: &CoordSeq,
        offsets: &CsrOffsetColumn,
        mut keep_mask: impl FnMut(&[f64], &[f64], &mut Vec<bool>) -> crate::error::Result<Option<usize>>,
    ) -> Result<Self> {
        let (xs, ys) = (coords.xs(), coords.ys());
        let rows = line_logical_len(offsets.as_slice(), RowSelectionRef::Identity);
        let mut builder = PackedColumnBuilder::like(coords, xs.len(), rows);
        let mut keep: Vec<bool> = Vec::new();
        for row in 0..rows {
            let window = PackedRow::line(row, coords, offsets, RowSelectionRef::Identity)
                .vertex_window()
                .expect("packed line row has a coordinate window");
            let kept = keep_mask(
                column_window(xs, &window),
                column_window(ys, &window),
                &mut keep,
            )?;
            // Too short or nothing removable: the row passes through whole.
            if kept.is_none_or(|count| count == window.len()) {
                builder.push_window_admitted(coords, window)?;
                continue;
            }
            builder.push_kept_vertices_admitted(
                coords,
                window,
                &keep,
                kept.expect("a changed keep mask reports its survivor count"),
            );
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
            None if self.has_missing()
                && matches!(self.storage(), GeometryArrayStorage::Mixed(_)) =>
            {
                self.try_drop_missing()?.storage().raw_total_bounds()
            },
            None => self.storage().raw_total_bounds(),
        };
        if self.len() == 0 {
            return Ok(Vec::new());
        }
        let Some(bounds) = bounds else {
            return Ok(vec![u64::MAX; self.len()]);
        };
        let frame = curves::CurveFrame::new(bounds, level);
        let mut keys = Vec::with_capacity(self.len());
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
    #[expect(
        clippy::indexing_slicing,
        reason = "the key vector has one entry per row and sort keys are generated from its own index range"
    )]
    pub(crate) fn taken_by_keys(&self, keys: &[u64]) -> crate::error::Result<Self> {
        let mut order: Vec<_> = (0..keys.len()).collect();
        order.sort_unstable_by_key(|&row| (self.is_row_missing(row), keys[row], row));
        self.gather_logical_rows(&order)
    }

    pub fn items(&self) -> PyResult<Vec<PyGeometry>> {
        // Mixed no longer stores PyGeometry; materialize through the array
        // geometry_at cache so repeated items()/arr[i] share ShapeData.
        let mut items = Vec::with_capacity(self.len());
        for index in 0..self.len() {
            items.push(self.geometry_at(index).map_err(crate::PyErr::from)?);
        }
        Ok(items)
    }

    /// Map each row's shape to a `Vec<Shape>` and collect into a per-row
    /// [`Groups`](crate::py::vectors::Groups): each input row's outputs form one
    /// ragged group. Missing rows have empty CSR windows and remain distinguishable
    /// through the Groups validity mask. One detached pass builds flat values and
    /// per-row offsets together.
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
                let mut builder = crate::CsrBuilder::<Shape>::with_capacity(0, array.len());
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
        Groups::from_geometry_builder(frame, builder, self.missing().cloned())
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
                let mut builder = crate::CsrBuilder::<Shape>::with_capacity(0, array.len());
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
        Groups::from_geometry_builder(frame, builder, self.missing().cloned())
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
        ) -> Result<crate::geometry::PublishedTriangleVertices>
        + Send,
    ) -> PyResult<Groups> {
        let array = self.clone();
        let frame = self.frame.clone();
        let builder = py
            .detach(move || {
                let mut builder = crate::CsrBuilder::<Point>::with_capacity(0, array.len());
                let mut ring_ends = Vec::new();
                let mut polygon_count = 0_usize;
                for logical_row in array.row_operation().rows_with_missing(Vec::<Point>::new) {
                    let (row, values) = match logical_row {
                        ArrayRow::Missing { logical, .. } => {
                            (logical, crate::geometry::PublishedTriangleVertices {
                                points: Vec::new(),
                                ring_ends: Vec::new(),
                                mesh_publication: None,
                            })
                        },
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
                    let mut previous_end = 0;
                    if values.ring_ends.iter().any(|&end| {
                        let invalid = end <= previous_end || end > values.points.len();
                        previous_end = end;
                        invalid
                    }) || previous_end != values.points.len()
                    {
                        return Err((
                            row,
                            crate::geometry::GeometryErrorKind::Invalid(
                                "triangle ring offsets do not match the vertex stream".into(),
                            )
                            .into(),
                        ));
                    }
                    let base = builder.len();
                    let row_polygon_count = values.ring_ends.len();
                    for value in values.points {
                        builder.values_mut().push(value);
                    }
                    for end in values.ring_ends {
                        ring_ends.push(base.checked_add(end).ok_or_else(|| {
                            (row, crate::error::ResourceError::SizeOverflow.into())
                        })?);
                    }
                    polygon_count = polygon_count
                        .checked_add(row_polygon_count)
                        .ok_or_else(|| (row, crate::error::ResourceError::SizeOverflow.into()))?;
                    builder
                        .push_end(polygon_count)
                        .map_err(|error| (row, error))?;
                }
                Ok::<_, (usize, crate::error::Error)>((builder, ring_ends))
            })
            .map_err(rows_err)?;
        let (points, ring_ends) = builder;
        Groups::from_point_builder(frame, points, &ring_ends, self.missing().cloned())
    }

    pub(crate) fn boundary_unary_packed(&self) -> Option<PyResult<Self>> {
        if self.missing().is_some() {
            return None;
        }
        let domain = match crate::geometry::topology_domain(&self.frame) {
            Ok(domain) => domain,
            Err(error) => return Some(Err(error.into())),
        };
        if domain.is_geographic() {
            return None;
        }
        if let GeometryArrayStorage::Packed(PackedStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        }) = self.storage()
            && ring_offsets.len() == polygon_offsets.len()
        {
            let valid = match self.is_valid_unary_packed() {
                Some(Ok(valid)) => valid,
                Some(Err(error)) => return Some(Err(error)),
                None => return None,
            };
            if !valid.into_iter().all(|valid| valid) {
                return None;
            }
            return Some(
                Self::from_storage(
                    GeometryArrayStorage::Packed(PackedStorage::Lines {
                        coords: Arc::clone(coords),
                        offsets: ring_offsets.clone().cast_level::<()>(),
                        row_map: row_map.clone(),
                    }),
                    self.frame.clone(),
                )
                .map_err(PyErr::from),
            );
        }
        None
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
                self.rows.gather_point_coords()?,
            )?)
        }))
    }
}

#[path = "map_shapes.rs"]
mod map_shapes;

#[cfg(test)]
mod geographic_validity_admission_tests {
    use pyo3::prelude::*;

    use super::PyGeometryArray;
    use crate::crs::frame::{Frame, crs_arc_static};
    use crate::io::parse_wkt;
    use crate::py::errors::InvalidGeometryError;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn out_of_domain_latitude_is_false_in_array_validity_without_raising() {
        Python::initialize();
        let invalid = parse_wkt("POLYGON ((0 0, 10 0, 10 100, 0 0))").unwrap();
        let valid = parse_wkt("POLYGON ((0 0, 10 0, 10 10, 0 0))").unwrap();
        let array = PyGeometryArray::from_shapes(
            vec![invalid, valid],
            Frame::Crs(crs_arc_static("EPSG:4326")),
        )
        .unwrap();
        Python::attach(|py| {
            let valid = array
                .is_valid(py)
                .unwrap()
                .bind(py)
                .call_method0("tolist")
                .unwrap()
                .extract::<Vec<bool>>()
                .unwrap();
            assert_eq!(valid, [false, true]);
            let error = array
                .is_simple(py)
                .expect_err("is_simple raises on out-of-domain latitude");
            assert!(error.is_instance_of::<InvalidGeometryError>(py));
        });
    }
}
