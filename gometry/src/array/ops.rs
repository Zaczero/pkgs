//! Internal `GeometryArray` operation helpers.
//!
//! Free `#[pyfunction]` surfaces call these instead of exposing thin method
//! delegates on `GeometryArray`.

use pyo3::exceptions::PyOverflowError;
use pyo3::types::{PyBytes, PyTuple};

use crate::array::{
    Arc, ArrayRow, Bound, CollectRows as _, DefaultedF64Input, Error, F64Param, Frame,
    GeometryArrayStorage, I64Param, InvalidGeometryError, PackedStorage, Py, PyAny,
    PyAnyMethods as _, PyErr, PyGeometryArray, PyResult, PyValidationReport, Python, RepairMethod,
    Result, Shape, VoronoiClipInput, bool_array, cdt_refinement_values, exact_geometry,
    exact_geometry_array, expected_geometry_or_array, geometry, io, non_negative_int,
    note_array_row, owned_voronoi_boundary, pair_packed_equals_exact, parse_precision,
    parse_wkt_output_dimension, require_geojson_crs, row_sample_seed, rows_err,
    validate_equals_exact_tolerance, validate_subdivide_max_vertices,
};
use crate::py::support;
use crate::py::vectors::Groups;

impl PyGeometryArray {
    pub(crate) fn to_wkt_impl(
        &self,
        py: Python<'_>,
        output_dimension: Option<&Bound<'_, PyAny>>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<String>> {
        let output_dimension = parse_wkt_output_dimension(output_dimension)?;
        let precision = precision.map(parse_precision).transpose()?;
        let array = self.clone();
        let crs = self.crs_ref().cloned();
        py.detach(move || {
            array
                .row_operation()
                .rows_with_missing(String::new)
                .map(|row| match row {
                    ArrayRow::Missing { value, .. } => Ok(value),
                    ArrayRow::Present(row) => row.row().try_with_shape(|shape| {
                        let shape = match precision {
                            Some(precision) => std::borrow::Cow::Owned(shape.quantize(precision)?),
                            None => std::borrow::Cow::Borrowed(shape),
                        };
                        io::to_wkt_with_dimension(
                            &shape,
                            output_dimension,
                            crs.as_deref(),
                            include_srid,
                        )
                    }),
                })
                .collect_rows()
        })
        .map_err(rows_err)
    }

    pub(crate) fn to_wkb_impl(
        &self,
        py: Python<'_>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<Py<PyBytes>>> {
        // Peak-bounded WITHOUT row staging: each detached block encodes
        // borrowed row views straight into one chunk buffer (no per-row owned
        // `Shape` materialization — that staging measured 17-25% slower), and
        // the block's `PyBytes` materialize between detaches so the Rust
        // buffer never holds more than ~one flush of payload. Small exports
        // take exactly one block, i.e. the classic single-buffer fast path.
        const FLUSH_BYTES: usize = 32 << 20;
        let precision = precision.map(parse_precision).transpose()?;
        let crs = self.crs_ref().cloned();
        let len = self.len();
        // An array CRS is uniform across present rows. Resolve it once, but
        // retain the old all-missing behavior by skipping validation when no
        // row can emit a payload. Attach the same first-present row note as
        // the row loop when resolution itself fails.
        let has_present = len != 0 && self.missing().is_none_or(|mask| mask.present_count() != 0);
        let srid = if include_srid && has_present {
            match crate::io::extended_srid_code(crs.as_deref(), true, "EWKB") {
                Ok(srid) => srid,
                Err(error) => {
                    let row = self.missing().map_or(0, |mask| {
                        mask.iter().position(|missing| !missing).unwrap_or(0)
                    });
                    return Err(rows_err((row, error)));
                },
            }
        } else {
            None
        };
        let mut output: Vec<Py<PyBytes>> = crate::try_vec_with_capacity(len)?;
        let mut next_row = 0_usize;
        while next_row < len {
            let array = self.clone();
            let start_row = next_row;
            let (buffer, ends) = py
                .detach(
                    move || -> Result<(Vec<u8>, Vec<usize>), (usize, crate::error::Error)> {
                        let mut buffer: Vec<u8> = Vec::new();
                        let mut ends: Vec<usize> = Vec::new();
                        let rows = array.row_operation();
                        for (offset, logical_row) in rows
                            .rows_with_missing(String::new)
                            .skip(start_row)
                            .enumerate()
                        {
                            let row = start_row + offset;
                            if let ArrayRow::Present(view) = logical_row {
                                let view = view.row();
                                view.try_with_shape(|shape| {
                                    let shape = match precision {
                                        Some(precision) => {
                                            std::borrow::Cow::Owned(shape.quantize(precision)?)
                                        },
                                        None => std::borrow::Cow::Borrowed(shape),
                                    };
                                    io::write_wkb_to_srid(&mut buffer, &shape, srid)
                                })
                                .map_err(|error| (row, error))?;
                            }
                            if let Err(error) = crate::try_push_vec(&mut ends, buffer.len()) {
                                drop(buffer);
                                return Err((row, error));
                            }
                            if buffer.len() >= FLUSH_BYTES {
                                break;
                            }
                        }
                        Ok((buffer, ends))
                    },
                )
                .map_err(rows_err)?;
            next_row = start_row
                .checked_add(ends.len())
                .ok_or_else(|| PyOverflowError::new_err("WKB row count overflows usize"))?;
            let mut start = 0;
            for end in ends {
                let bytes = PyBytes::new(py, &buffer[start..end]).unbind();
                start = end;
                if let Err(error) = crate::try_push_vec(&mut output, bytes) {
                    drop(buffer);
                    return Err(pyo3::PyErr::from(error));
                }
            }
        }
        Ok(output)
    }

    pub(crate) fn to_geojson_impl(&self, py: Python<'_>, include_z: bool) -> PyResult<Vec<String>> {
        require_geojson_crs(self.crs_str())?;
        // O(1) axis probe off the packed columns instead of materializing every
        // row's `Shape`. M is unrepresentable in GeoJSON: refuse it by default
        // (no silent loss).
        if self.has_m() {
            return Err(InvalidGeometryError::new_err(
                "GeoJSON has no M ordinate; remove M with set_m(None), or use WKT/GeoArrow",
            ));
        }
        let array = self.clone();
        // GeoJSON treats CRS-free coordinates as WGS84 lon/lat too. A
        // domain-invalid row is a spec violation, not a degradable data
        // condition, so it fails the batch (like the M-ordinate refusal above).
        let frame = self.frame.clone();
        Ok(if include_z {
            py.detach(move || {
                let domain = io::geojson_domain(frame.crs_str())?;
                let mut out = Vec::new();
                for row in array.row_operation().rows_with_missing(String::new) {
                    let value = match row {
                        ArrayRow::Missing { value, .. } => value,
                        ArrayRow::Present(row) => row
                            .row()
                            .try_with_shape(|shape| io::to_geojson_string::<true>(shape, domain))?,
                    };
                    crate::boundary::try_push_vec(&mut out, value)?;
                }
                Ok::<Vec<String>, crate::error::Error>(out)
            })?
        } else {
            let frame = self.frame.clone();
            py.detach(move || {
                let domain = io::geojson_domain(frame.crs_str())?;
                let mut out = Vec::new();
                for row in array.row_operation().rows_with_missing(String::new) {
                    let value = match row {
                        ArrayRow::Missing { value, .. } => value,
                        ArrayRow::Present(row) => row.row().try_with_shape(|shape| {
                            io::to_geojson_string::<false>(shape, domain)
                        })?,
                    };
                    crate::boundary::try_push_vec(&mut out, value)?;
                }
                Ok::<Vec<String>, crate::error::Error>(out)
            })?
        })
    }

    pub(crate) fn validate_impl(
        &self,
        py: Python<'_>,
    ) -> PyResult<Vec<Option<PyValidationReport>>> {
        let domain = geometry::topology_domain(&self.frame)?;
        // Validating every row is the same native work `repair` detaches.
        py.detach(|| {
            let mut reports = Vec::new();
            crate::boundary::try_reserve_vec(&mut reports, self.len())?;
            let mut rows = self.row_operation();
            for row in rows.rows_with_missing(|| ()) {
                let report = match row {
                    ArrayRow::Missing { .. } => None,
                    ArrayRow::Present(row) => {
                        let geometry = rows.geometry(row).map_err(PyErr::from)?;
                        let plan = geometry.prepared_plan();
                        let issue = crate::geometry::validate_data_in_frame(&plan, domain)?;
                        Some(PyValidationReport { geometry, issue })
                    },
                };
                crate::try_push_vec(&mut reports, report)?;
            }
            Ok(reports)
        })
    }

    pub(crate) fn repair_impl(&self, py: Python<'_>, method: RepairMethod) -> PyResult<Self> {
        let array = self.clone();
        let domain = geometry::topology_domain(&self.frame)?;
        let repaired = py
            .detach(move || {
                let mut all_present_valid = true;
                let mut shapes = Vec::new();
                for logical_row in array.row_operation().rows_with_missing(|| ()) {
                    let ArrayRow::Present(row) = logical_row else {
                        continue;
                    };
                    let logical = row.logical();
                    let shape = row.row();
                    match shape
                        .try_with_shape(|shape| {
                            geometry::repair_shape_in_frame(shape, domain, method)
                        })
                        .map_err(|error| (logical, error))?
                    {
                        None => crate::boundary::try_push_vec(
                            &mut shapes,
                            shape
                                .with_shape(Clone::clone)
                                .map_err(|error| (logical, error))?,
                        )
                        .map_err(|error| (logical, error))?,
                        Some(repaired) => {
                            all_present_valid = false;
                            crate::boundary::try_push_vec(&mut shapes, repaired)
                                .map_err(|error| (logical, error))?;
                        },
                    }
                }
                Ok(if all_present_valid {
                    None
                } else {
                    Some(shapes)
                })
            })
            .map_err(rows_err)?;
        let output = match repaired {
            None => self.clone(),
            Some(shapes) => self
                .scatter_present_result(Self::from_shapes(shapes, self.frame.clone())?)
                .map_err(PyErr::from)?,
        };
        Ok(output)
    }

    pub(crate) fn self_intersections_impl(&self, py: Python<'_>) -> PyResult<Groups> {
        Groups::from_self_intersection_rows(py, self)
    }

    pub(crate) fn equals_exact(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        tolerance: DefaultedF64Input,
        include_z: bool,
        include_m: bool,
    ) -> PyResult<Py<PyAny>> {
        let tolerance = tolerance.resolve(py, "tolerance", self.len())?;
        tolerance.try_validate(|value| validate_equals_exact_tolerance(value).map(|_| ()))?;
        // Same preconditions as every binary op — the packed fast path used
        // to skip both, silently comparing across frames and truncating to
        // the shorter operand.
        if let Some(other_array) = exact_geometry_array(other) {
            crate::broadcast::ensure_same_len(self.len(), other_array.len())?;
            Frame::compatible_parts(
                self.crs_ref(),
                self.epoch(),
                other_array.crs_ref(),
                other_array.epoch(),
                "equals_exact",
            )?;
        } else if let Some(geometry) = exact_geometry(other) {
            Frame::compatible_parts(
                self.crs_ref(),
                self.epoch(),
                geometry.crs_ref(),
                geometry.epoch(),
                "equals_exact",
            )?;
        }
        if let Some(scalar_tol) = tolerance.as_scalar() {
            if let Some(other_array) = exact_geometry_array(other)
                && !self.is_compact_nullable()
                && !other_array.is_compact_nullable()
                && let Some(mut results) = pair_packed_equals_exact(
                    py,
                    self,
                    other_array,
                    scalar_tol,
                    include_z,
                    include_m,
                )?
            {
                if let Some(mask) = self.missing() {
                    for (result, is_missing) in results.iter_mut().zip(mask.iter()) {
                        if is_missing {
                            *result = false;
                        }
                    }
                }
                if let Some(mask) = &other_array.missing() {
                    for (result, is_missing) in results.iter_mut().zip(mask.iter()) {
                        if is_missing {
                            *result = false;
                        }
                    }
                }
                return bool_array(py, results);
            }
            let results = match (include_z, include_m) {
                (false, false) => {
                    array_equals_exact_scalar_tolerance::<false, false>(py, self, other, scalar_tol)
                },
                (true, false) => {
                    array_equals_exact_scalar_tolerance::<true, false>(py, self, other, scalar_tol)
                },
                (false, true) => {
                    array_equals_exact_scalar_tolerance::<false, true>(py, self, other, scalar_tol)
                },
                (true, true) => {
                    array_equals_exact_scalar_tolerance::<true, true>(py, self, other, scalar_tol)
                },
            }?;
            return bool_array(py, results);
        }
        let results = match (include_z, include_m) {
            (false, false) => {
                array_equals_exact_row_tolerance::<false, false>(py, self, other, &tolerance)
            },
            (true, false) => {
                array_equals_exact_row_tolerance::<true, false>(py, self, other, &tolerance)
            },
            (false, true) => {
                array_equals_exact_row_tolerance::<false, true>(py, self, other, &tolerance)
            },
            (true, true) => {
                array_equals_exact_row_tolerance::<true, true>(py, self, other, &tolerance)
            },
        }?;
        bool_array(py, results)
    }

    pub(crate) fn delaunay_triangles_impl(&self, py: Python<'_>) -> PyResult<Groups> {
        // Mixed-axis rows cannot land in one packed coords column (`from_points`
        // assumes axis-homogeneity); per-shape `delaunay_triangles` preserves
        // each row's ordinate layout via `carry_each`.
        if self.uniform_axes()?.is_none() {
            return self.flat_map_shapes_groups_budgeted(
                py,
                "triangulate",
                "method",
                |_, shape, budget| shape.delaunay_triangles_budgeted(budget),
            );
        }
        // Mirror the scalar `Geometry::delaunay_triangles` packed builder: each
        // row's flat `[a, c, b, a]` vertex stream is concatenated into ONE
        // coords column + arithmetic CSR offsets — skipping per-triangle
        // `Polygon`/`CoordSeq` materialization. Per-row `Groups` offsets are the
        // running triangle counts, so grouping is free.
        self.flat_map_packed_triangles_groups_budgeted(
            py,
            "triangulate",
            "method",
            |_, shape, budget| shape.delaunay_triangle_vertices_budgeted(budget),
        )
    }

    pub(crate) fn constrained_delaunay_triangles_impl(
        &self,
        py: Python<'_>,
        min_angle: Option<&Bound<'_, PyAny>>,
        max_area: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Groups> {
        let len = self.len();
        let min_angle = min_angle
            .map(|value| F64Param::parse(value, "min_angle", len))
            .transpose()?;
        let max_area = max_area
            .map(|value| F64Param::parse(value, "max_area", len))
            .transpose()?;
        let refinements = (0..len)
            .map(|row| {
                cdt_refinement_values(
                    min_angle.as_ref().map(|value| value.get(row)),
                    max_area.as_ref().map(|value| value.get(row)),
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        // Unrefined rows preserve the selected source-site axes while refined
        // rows are XY, so a batch carrying Z/M uses the shape collector.
        // Pure-XY batches retain the packed triangle builder; both consume the
        // same constrained mesh publisher.
        if self.has_z() || self.has_m() {
            return self.flat_map_shapes_groups_budgeted(
                py,
                "triangulate",
                "min_angle/max_area",
                move |row, shape, budget| {
                    shape.constrained_delaunay_triangles_budgeted(refinements[row], budget)
                },
            );
        }
        self.flat_map_packed_triangles_groups_budgeted(
            py,
            "triangulate",
            "min_angle/max_area",
            move |row, shape, budget| {
                shape.constrained_delaunay_vertices_budgeted(refinements[row], budget)
            },
        )
    }

    pub(crate) fn polygon_triangles_impl(&self, py: Python<'_>) -> PyResult<Groups> {
        if self.earcut_rows_pack()? {
            return self.flat_map_packed_triangles_groups_budgeted(
                py,
                "triangulate",
                "method",
                |_, shape, budget| shape.polygon_triangle_vertices_budgeted(budget),
            );
        }
        self.flat_map_shapes_groups_budgeted(py, "triangulate", "method", |_, shape, budget| {
            shape.polygon_triangles_budgeted(budget)
        })
    }

    fn earcut_rows_pack(&self) -> crate::error::Result<bool> {
        Ok(match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Polygons { .. }) => true,
            GeometryArrayStorage::Packed(_) => false,
            GeometryArrayStorage::Mixed(_) => {
                let mut axes = None;
                for row in self.present_shape_rows() {
                    let (_, shape) = row?;
                    let Some(row) = shape.earcut_packed_axes() else {
                        return Ok(false);
                    };
                    match axes {
                        None => axes = Some(row),
                        Some(prev) if prev == row => {},
                        Some(_) => return Ok(false),
                    }
                }
                axes.is_some()
            },
        })
    }

    pub(crate) fn sample_points_impl(
        &self,
        count: &Bound<'_, PyAny>,
        seed: &Bound<'_, PyAny>,
    ) -> PyResult<Groups> {
        let len = self.len();
        let count = I64Param::parse(count, "count", len)?;
        count
            .try_validate(|value| non_negative_int("sample_points", "count", value).map(|_| ()))?;
        let seed = I64Param::parse(seed, "seed", len)?;
        let mut total = 0_usize;
        for (row, missing) in (0..len).map(|row| (row, self.is_row_missing(row))) {
            if !missing {
                let count = usize::try_from(count.get(row)).expect("validated non-negative");
                crate::geometry::ExpansionBudget::check("sample_points", "count", count)?;
                total = total
                    .checked_add(count)
                    .ok_or_else(|| Error::from(crate::error::ResourceError::SizeOverflow))?;
            }
        }
        let built = (|| -> Result<crate::CsrBuilder<Shape>, (Error, Option<usize>)> {
            let mut builder = crate::CsrBuilder::<Shape>::with_capacity(total, len);
            for logical_row in self.row_operation().rows_with_missing(|| ()) {
                let (row, values) = match logical_row {
                    crate::array::ArrayRow::Missing { logical, .. } => (logical, None),
                    crate::array::ArrayRow::Present(row) => {
                        let logical = row.logical();
                        let count =
                            usize::try_from(count.get(logical)).expect("validated non-negative");
                        // One rule: a geometry seeded with `s` always draws
                        // `row_sample_seed(s, 0)`. A single seed spreads across rows by
                        // index; per-row seeds are each that row's OWN seed, so each
                        // draws the stream a scalar with that seed draws. Both keep
                        // rows independent AND agree with the scalar surface.
                        let seed = match &seed {
                            I64Param::Scalar(value) => row_sample_seed(*value as u64, logical),
                            I64Param::PerElement(values) => {
                                row_sample_seed(values[logical] as u64, 0)
                            },
                        };
                        let points = row
                            .row()
                            .try_with_shape(|shape| shape.sample_points(count, seed))
                            .map_err(|error| (error, None))?;
                        (logical, Some(points))
                    },
                };
                builder
                    .push_row(values.into_iter().flatten().map(Shape::Point))
                    .map_err(|error| (error, Some(row)))?;
            }
            Ok(builder)
        })();
        let builder = match built {
            Ok(builder) => builder,
            Err((error, None)) => return Err(error.into()),
            Err((error, Some(row))) => return Err(note_array_row(error.into(), row)),
        };
        Groups::from_geometry_builder(self.frame.clone(), builder)
    }

    pub(crate) fn voronoi_polygons_impl(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<Groups> {
        let boundary = owned_voronoi_boundary(py, clip, &self.frame, "voronoi_polygons")?;
        self.flat_map_shapes_groups_budgeted(
            py,
            "voronoi_polygons",
            "sites/clip topology",
            move |_, shape, budget| {
                shape.voronoi_polygons_budgeted(tolerance, boundary.as_borrowed(), budget)
            },
        )
    }

    pub(crate) fn voronoi_edges_impl(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<Groups> {
        let boundary = owned_voronoi_boundary(py, clip, &self.frame, "voronoi_edges")?;
        let array = self.clone();
        let frame = self.frame.clone();
        let packed = py
            .detach(move || {
                let mut columns = Vec::new();
                let mut group_offsets = vec![0_i64];
                for logical_row in array.row_operation().rows_with_missing(|| ()) {
                    let (row, n_lines) = match logical_row {
                        ArrayRow::Missing { logical, .. } => (logical, 0_usize),
                        ArrayRow::Present(row) => {
                            let logical = row.logical();
                            let (coords, offsets) = row
                                .row()
                                .try_with_shape(|shape| {
                                    shape.voronoi_edge_column_budgeted(
                                        tolerance,
                                        boundary.as_borrowed(),
                                        &mut crate::geometry::ExpansionBudget::new(
                                            "voronoi_edges",
                                            "sites/clip topology",
                                        ),
                                    )
                                })
                                .map_err(|error| (logical, error))?;
                            let n_lines = offsets.len().saturating_sub(1);
                            if n_lines > 0 {
                                columns.push((coords, offsets));
                            }
                            (logical, n_lines)
                        },
                    };
                    let next = group_offsets
                        .last()
                        .copied()
                        .unwrap_or(0)
                        .checked_add(i64::try_from(n_lines).map_err(|_| {
                            (
                                row,
                                crate::geometry::GeometryErrorKind::message(
                                    "Voronoi edge group overflow",
                                ),
                            )
                        })?)
                        .ok_or_else(|| {
                            (
                                row,
                                crate::geometry::GeometryErrorKind::message(
                                    "Voronoi edge group overflow",
                                ),
                            )
                        })?;
                    group_offsets.push(next);
                }
                Ok((columns, group_offsets))
            })
            .map_err(rows_err)?;
        let (columns, group_offsets) = packed;
        let (coords, line_offsets) =
            py.detach(move || crate::array::concat_packed_line_columns(columns))?;
        Groups::from_packed_line_groups(frame, coords, line_offsets, group_offsets)
    }

    /// Per-row polygonize: each input geometry's own linework → its polygons,
    /// one group per input (`= scalar polygonize per row`). Pool raw linework
    /// with the free function ``polygonize``; passing a GeometryArray there
    /// pools its present rows.
    pub(crate) fn polygonize_impl(&self, py: Python<'_>) -> PyResult<Groups> {
        self.flat_map_shapes_groups(py, move |shape| shape.polygonize(false))
    }

    pub(crate) fn line_merge_impl(&self, py: Python<'_>) -> PyResult<Self> {
        if let Some(identity) = self.packed_lines_identity() {
            return Ok(identity);
        }
        self.map_shapes_detached(py, Shape::line_merge)
    }

    /// `tolerance` is already validated non-negative at the free-function boundary.
    pub(crate) fn split(
        &self,
        py: Python<'_>,
        splitter: &Bound<'_, PyAny>,
        tolerance: f64,
    ) -> PyResult<Self> {
        let shapes = if let Some(splitter) = exact_geometry(splitter) {
            Frame::compatible_parts(
                self.crs_ref(),
                self.epoch(),
                splitter.crs_ref(),
                splitter.epoch(),
                "split",
            )?;
            let splitter_shape = Arc::clone(&splitter.shape);
            let array = self.clone();
            py.detach(move || {
                let mut shapes = Vec::new();
                for row in array.present_shape_rows() {
                    let (_, line) = row?;
                    shapes.extend(line.split(&splitter_shape, tolerance)?);
                }
                Ok::<_, Error>(shapes)
            })?
        } else if let Some(splitters) = exact_geometry_array(splitter) {
            crate::broadcast::validate_paired_arrays(self, splitters, "split")?;
            let lines = self.clone();
            let cutters = splitters.clone();
            py.detach(move || {
                let mut shapes = Vec::new();
                for (line, cutter) in lines
                    .row_operation()
                    .rows_with_missing(|| ())
                    .zip(cutters.row_operation().rows_with_missing(|| ()))
                {
                    let (ArrayRow::Present(line), ArrayRow::Present(cutter)) = (line, cutter)
                    else {
                        continue;
                    };
                    line.row().try_with_shape(|line| {
                        cutter.row().try_with_shape(|cutter| {
                            shapes.extend(line.split(cutter, tolerance)?);
                            Ok::<_, Error>(())
                        })
                    })?;
                }
                Ok::<_, Error>(shapes)
            })?
        } else {
            return Err(expected_geometry_or_array());
        };
        Ok(Self::from_shapes(shapes, self.frame.clone())?)
    }

    pub(crate) fn extremes_impl(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let constructor = support::extreme_points_type(py)?;
        // One vertex scan per present row feeds all four direction columns;
        // empty rows degrade to an empty point per column (the geometry-lane
        // sentinel) and missing rows stay missing via the scatter machinery.
        let mut columns: [Vec<Shape>; 4] = std::array::from_fn(|_| Vec::new());
        for row in self.present_shape_rows() {
            let (_, shape) = row?;
            match shape.extremes() {
                Some(points) => {
                    for (column, point) in columns.iter_mut().zip(points) {
                        column.push(Shape::Point(point));
                    }
                },
                None => {
                    for column in &mut columns {
                        column.push(Shape::empty_point());
                    }
                },
            }
        }
        let arrays = columns
            .into_iter()
            .map(|shapes| {
                self.scatter_present_result(Self::from_shapes(shapes, self.frame.clone())?)
                    .map_err(PyErr::from)
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(constructor.call1(PyTuple::new(py, arrays)?)?.unbind())
    }

    pub(crate) fn subdivide_rows(&self, py: Python<'_>, max_vertices: i64) -> PyResult<Groups> {
        let max_vertices = validate_subdivide_max_vertices(max_vertices)?;
        // Per-row groups: each input geometry's subdivided parts stay grouped
        // under their source row. `iter_shapes` gives packed rows CSR views with
        // no Mixed boxing, so the group build is as cheap as the flat path was.
        self.flat_map_shapes_groups(py, move |shape| shape.subdivide(max_vertices, false))
    }
}

fn array_equals_exact_scalar_tolerance<const Z: bool, const M: bool>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &Bound<'_, PyAny>,
    tolerance: f64,
) -> PyResult<Vec<bool>> {
    crate::broadcast::array_binary_values_indexed(
        py,
        left,
        right,
        "equals_exact",
        move |left, right, _row| Ok(left.equals_exact_impl::<Z, M>(right, tolerance)),
    )
}

fn array_equals_exact_row_tolerance<const Z: bool, const M: bool>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &Bound<'_, PyAny>,
    tolerance: &F64Param,
) -> PyResult<Vec<bool>> {
    crate::broadcast::array_binary_values_indexed(
        py,
        left,
        right,
        "equals_exact",
        move |left, right, row| Ok(left.equals_exact_impl::<Z, M>(right, tolerance.get(row))),
    )
}

#[cfg(test)]
mod generation_budget_tests {
    use super::*;
    use crate::Point;

    #[test]
    fn each_row_gets_its_own_generation_budget() {
        crate::test_support::initialize_python();
        let array = PyGeometryArray::from_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(0.0, 0.0)),
                Shape::Point(Point::new_raw_xy(1.0, 0.0)),
            ],
            Frame::new(None, None).unwrap(),
        )
        .unwrap();
        let charge = |amount: usize| {
            Python::attach(|py| {
                array.flat_map_shapes_groups_budgeted(
                    py,
                    "voronoi_polygons",
                    "sites/clip topology",
                    move |_, _, budget| {
                        budget.add(amount)?;
                        Ok(Vec::new())
                    },
                )
            })
        };
        // Together the rows exceed the limit; each stays within it.
        charge(crate::geometry::GENERATED_ITEM_LIMIT / 2 + 1).unwrap();
        assert!(charge(crate::geometry::GENERATED_ITEM_LIMIT + 1).is_err());
    }
}
