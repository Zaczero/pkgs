#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Internal `GeometryArray` operation helpers.
//!
//! Free `#[pyfunction]` surfaces call these instead of exposing thin method
//! delegates on `GeometryArray`.

use pyo3::types::{PyBytes, PyTuple};

use crate::array::{
    Arc, Bound, CollectRows as _, CoordinateAxes, DefaultedF64Input, DistanceUnit, Error, F64Param,
    Frame, I64Param, InvalidGeometryError, Py, PyAny, PyAnyMethods as _, PyGeometryArray, PyResult,
    PyValidationReport, Python, RepairMethod, Result, Shape, VoronoiClipInput,
    array_binary_geometry, bool_array, cdt_refinement_values, crs, curves, exact_geometry,
    exact_geometry_array, expected_geometry_or_array, fixed_geometry_array_nearest_points,
    geometry, io, metric_nearest_points, metric_shortest_line, non_negative_int, note_array_row,
    owned_voronoi_boundary, pair_packed_equals_exact, paired_arrays, parse_precision,
    parse_wkt_output_dimension, require_geojson_crs, resolve_metric, row_sample_seed, rows_err,
    validate_equals_exact_tolerance, validate_subdivide_max_vertices,
};

impl PyGeometryArray {
    /// Per-row export rows as a Python list with ``None`` at missing rows —
    /// the masked exit shared by ``to_wkt``/``to_wkb``/``to_geojson``.
    pub(crate) fn masked_row_list<T>(&self, py: Python<'_>, rows: Vec<T>) -> PyResult<Py<PyAny>>
    where
        T: for<'py> pyo3::IntoPyObjectExt<'py>,
    {
        use pyo3::IntoPyObjectExt as _;
        match self.missing() {
            None => rows.into_py_any(py),
            Some(mask) => {
                let rows: Vec<Py<PyAny>> = rows
                    .into_iter()
                    .enumerate()
                    .map(|(row, value)| {
                        if mask[row] {
                            Ok(py.None())
                        } else {
                            value.into_py_any(py)
                        }
                    })
                    .collect::<PyResult<_>>()?;
                rows.into_py_any(py)
            },
        }
    }

    pub(crate) fn to_wkt_impl(
        &self,
        py: Python<'_>,
        output_dimension: Option<&Bound<'_, PyAny>>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<String>> {
        let output_dimension = parse_wkt_output_dimension(output_dimension)?;
        let precision = precision.map(parse_precision).transpose()?;
        let shapes = Arc::clone(self.storage_arc());
        let crs = self.crs_ref().cloned();
        let missing = self.missing().cloned();
        py.detach(move || {
            shapes
                .iter_shapes()
                .enumerate()
                .map(|shape| {
                    let (row, shape) = shape;
                    if missing.as_ref().is_some_and(|mask| mask[row]) {
                        return Ok(String::new());
                    }
                    let shape = match precision {
                        Some(precision) => std::borrow::Cow::Owned(shape.quantize(precision)),
                        None => shape,
                    };
                    io::to_wkt_with_dimension(
                        &shape,
                        output_dimension,
                        crs.as_deref(),
                        include_srid,
                    )
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
        let len = self.storage().len();
        let mut output: Vec<Py<PyBytes>> = Vec::with_capacity(len);
        let mut next_row = 0_usize;
        while next_row < len {
            let storage = Arc::clone(self.storage_arc());
            let missing = self.missing().cloned();
            let crs = crs.clone();
            let start_row = next_row;
            let (buffer, ends) = py
                .detach(
                    move || -> Result<(Vec<u8>, Vec<usize>), (usize, crate::error::Error)> {
                        let mut buffer: Vec<u8> = Vec::new();
                        let mut ends: Vec<usize> = Vec::new();
                        for (offset, view) in storage.iter_rows().skip(start_row).enumerate() {
                            let row = start_row + offset;
                            if !missing.as_ref().is_some_and(|mask| mask[row]) {
                                view.with_shape(|shape| {
                                    let shape = precision.map_or_else(
                                        || std::borrow::Cow::Borrowed(shape),
                                        |precision| {
                                            std::borrow::Cow::Owned(shape.quantize(precision))
                                        },
                                    );
                                    io::write_wkb_to(
                                        &mut buffer,
                                        &shape,
                                        crs.as_deref(),
                                        include_srid,
                                    )
                                })
                                .map_err(|error| (row, error))?;
                            }
                            ends.push(buffer.len());
                            if buffer.len() >= FLUSH_BYTES {
                                break;
                            }
                        }
                        Ok((buffer, ends))
                    },
                )
                .map_err(rows_err)?;
            next_row = start_row + ends.len();
            let mut start = 0;
            output.extend(ends.into_iter().map(|end| {
                let bytes = PyBytes::new(py, &buffer[start..end]).unbind();
                start = end;
                bytes
            }));
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
        let shapes = Arc::clone(self.storage_arc());
        let missing = self.missing().cloned();
        // A WGS84-tagged frame opts into the RFC 7946 geographic rules (domain
        // validation, antimeridian cutting); CRS-free rows are planar. A
        // domain-invalid row is a spec violation, not a degradable data
        // condition, so it fails the batch (like the M-ordinate refusal above).
        let geographic = self.crs_str().is_some();
        Ok(if include_z {
            py.detach(move || {
                shapes
                    .iter_shapes()
                    .enumerate()
                    .map(|(row, shape)| {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            Ok(String::new())
                        } else {
                            io::to_geojson_string::<true>(&shape, geographic)
                        }
                    })
                    .collect::<crate::error::Result<Vec<String>>>()
            })?
        } else {
            py.detach(move || {
                shapes
                    .iter_shapes()
                    .enumerate()
                    .map(|(row, shape)| {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            Ok(String::new())
                        } else {
                            io::to_geojson_string::<false>(&shape, geographic)
                        }
                    })
                    .collect::<crate::error::Result<Vec<String>>>()
            })?
        })
    }

    pub(crate) fn validate_impl(&self) -> Vec<Option<PyValidationReport>> {
        let geographic = geometry::is_geographic_frame(&self.frame);
        (0..self.storage().len())
            .map(|index| {
                if self.is_row_missing(index) {
                    return None;
                }
                let geometry = self.geometry_at(index);
                let issue = crate::geometry::validate_data_in_frame(&geometry.shape, geographic);
                Some(PyValidationReport { geometry, issue })
            })
            .collect()
    }

    pub(crate) fn repair_impl(&self, py: Python<'_>, method: RepairMethod) -> PyResult<Self> {
        let storage = Arc::clone(self.storage_arc());
        let missing = self.missing().cloned();
        let geographic = geometry::is_geographic_frame(&self.frame);
        let repaired = py
            .detach(move || {
                let mut all_present_valid = true;
                let mut shapes = Vec::with_capacity(storage.len());
                for (row, shape) in storage.iter_shapes().enumerate() {
                    if missing.as_ref().is_some_and(|mask| mask[row]) {
                        shapes.push(shape.into_owned());
                        continue;
                    }
                    match geometry::repair_shape_in_frame(&shape, geographic, method)
                        .map_err(|error| (row, error))?
                    {
                        None => shapes.push(shape.into_owned()),
                        Some(repaired) => {
                            all_present_valid = false;
                            shapes.push(repaired);
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
        Ok(repaired.map_or_else(
            || self.clone(),
            |shapes| {
                Self::from_shapes(shapes, self.frame.clone())
                    .with_missing_mask(self.missing().cloned())
            },
        ))
    }

    pub(crate) fn self_intersections_impl(&self) -> PyResult<crate::py::vectors::Groups> {
        crate::py::vectors::Groups::from_self_intersection_rows(self)
    }

    pub(crate) fn equals_exact(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        tolerance: DefaultedF64Input,
        include_z: bool,
        include_m: bool,
    ) -> PyResult<Py<PyAny>> {
        let tolerance = tolerance.resolve(py, "tolerance", self.storage().len())?;
        tolerance.try_validate(|value| validate_equals_exact_tolerance(value).map(|_| ()))?;
        // Same preconditions as every binary op — the packed fast path used
        // to skip both, silently comparing across frames and truncating to
        // the shorter operand.
        if let Some(other_array) = exact_geometry_array(other) {
            crate::broadcast::ensure_same_len(self.storage().len(), other_array.storage().len())?;
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
                    for (result, &is_missing) in results.iter_mut().zip(mask.iter()) {
                        if is_missing {
                            *result = false;
                        }
                    }
                }
                if let Some(mask) = &other_array.missing() {
                    for (result, &is_missing) in results.iter_mut().zip(mask.iter()) {
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

    #[expect(
        clippy::too_many_lines,
        reason = "geodesic scalar/array and array/array lanes keep cache ownership explicit"
    )]
    pub(crate) fn shortest_line(
        &self,
        other: &Bound<'_, PyAny>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        let model = resolve_metric(self.crs_str(), unit, "shortest_line")?;
        if let crs::MetricModel::Geodesic(crs) = model {
            if let Some(geometry) = exact_geometry(other) {
                Frame::compatible_parts(
                    self.crs_ref(),
                    self.epoch(),
                    geometry.crs_ref(),
                    geometry.epoch(),
                    "shortest_line",
                )?;
                let lefts = self.clone();
                let right = Arc::clone(&geometry.shape);
                let right_cache = Arc::clone(&geometry.frame_cache);
                let missing = self.missing().cloned();
                let rows = Python::attach(|py| {
                    py.detach(move || {
                        crs::with_resolved_ellipsoid_metric(
                            &crs,
                            &[right.shape()],
                            |crs, metric| {
                                let (semi_major, flattening) = metric.ellipsoid_parameters();
                                right.prepare_geodesic_parts(
                                    &right_cache,
                                    crs,
                                    semi_major,
                                    flattening,
                                    metric,
                                )?;
                                Ok(lefts
                                    .storage()
                                    .iter_rows()
                                    .enumerate()
                                    .map(|(row_index, row)| {
                                        if missing.as_ref().is_some_and(|mask| mask[row_index]) {
                                            return Ok(Self::missing_placeholder());
                                        }
                                        let left = lefts.prepared_row(row_index, row);
                                        let left_cache = lefts.row_frame_cache(row_index);
                                        Ok(crate::geometry::nearest_line(
                                            left.geodesic_nearest_points_cached_split(
                                                &left_cache,
                                                &right,
                                                &right_cache,
                                                crs,
                                                semi_major,
                                                flattening,
                                                metric,
                                            )?,
                                            crate::geometry::common_axes(
                                                left.shape(),
                                                right.shape(),
                                            ),
                                        ))
                                    })
                                    .collect_rows())
                            },
                        )
                    })
                })?;
                return Ok(
                    Self::from_shapes(rows.map_err(rows_err)?, self.frame.clone())
                        .with_missing_mask(self.missing().cloned()),
                );
            }
            if let Some(array) = exact_geometry_array(other) {
                paired_arrays(self, array, "shortest_line")?;
                let (lefts, rights) = (self.clone(), array.clone());
                let missing = crate::array::missing::union_pair(self.missing(), array.missing());
                let missing_rows = missing.clone();
                let rows = Python::attach(|py| {
                    py.detach(move || {
                        crs::with_resolved_ellipsoid_metric(&crs, &[], |crs, metric| {
                            let (semi_major, flattening) = metric.ellipsoid_parameters();
                            Ok(lefts
                                .storage()
                                .iter_rows()
                                .zip(rights.storage().iter_rows())
                                .enumerate()
                                .map(|(row, (left, right))| {
                                    if missing_rows.as_ref().is_some_and(|mask| mask[row]) {
                                        return Ok(Self::missing_placeholder());
                                    }
                                    let left = lefts.prepared_row(row, left);
                                    let right = rights.prepared_row(row, right);
                                    let left_cache = lefts.row_frame_cache(row);
                                    let right_cache = rights.row_frame_cache(row);
                                    Ok(crate::geometry::nearest_line(
                                        left.geodesic_nearest_points_cached_split(
                                            &left_cache,
                                            &right,
                                            &right_cache,
                                            crs,
                                            semi_major,
                                            flattening,
                                            metric,
                                        )?,
                                        crate::geometry::common_axes(left.shape(), right.shape()),
                                    ))
                                })
                                .collect_rows())
                        })
                    })
                })?;
                return Ok(
                    Self::from_shapes(rows.map_err(rows_err)?, self.frame.clone())
                        .with_missing_mask(missing),
                );
            }
            return Err(expected_geometry_or_array());
        }
        Python::attach(|py| {
            array_binary_geometry(py, self, other, "shortest_line", move |left, right| {
                metric_shortest_line(&model, left, right)
            })
        })
    }

    pub(crate) fn nearest_points(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<(Self, Self)> {
        if let Some(geometry) = exact_geometry(other) {
            return fixed_geometry_array_nearest_points(py, geometry, self, false, unit);
        }
        if let Some(array) = exact_geometry_array(other) {
            paired_arrays(self, array, "nearest_points")?;
            let (lefts, rights) = (self.clone(), array.clone());
            let model = resolve_metric(self.crs_str(), unit, "nearest_points")?;
            let missing = crate::array::missing::union_pair(self.missing(), array.missing());
            if let crs::MetricModel::Geodesic(crs) = model {
                let missing_rows = missing.clone();
                let pairs = py
                    .detach(move || {
                        crs::with_resolved_ellipsoid_metric(&crs, &[], |crs, metric| {
                            let (semi_major, flattening) = metric.ellipsoid_parameters();
                            Ok(lefts
                                .storage()
                                .iter_rows()
                                .zip(rights.storage().iter_rows())
                                .enumerate()
                                .map(|(row, (left, right))| {
                                    if missing_rows.as_ref().is_some_and(|mask| mask[row]) {
                                        return Ok((None, CoordinateAxes::XY));
                                    }
                                    let left = lefts.prepared_row(row, left);
                                    let right = rights.prepared_row(row, right);
                                    let left_cache = lefts.row_frame_cache(row);
                                    let right_cache = rights.row_frame_cache(row);
                                    let common =
                                        crate::geometry::common_axes(left.shape(), right.shape());
                                    left.geodesic_nearest_points_cached_split(
                                        &left_cache,
                                        &right,
                                        &right_cache,
                                        crs,
                                        semi_major,
                                        flattening,
                                        metric,
                                    )
                                    .map(|pair| (pair, common))
                                })
                                .collect_rows())
                        })
                    })?
                    .map_err(rows_err)?;
                return Ok(crate::py::support::nearest_point_columns_masked(
                    pairs,
                    self.frame.clone(),
                    missing,
                ));
            }
            let missing_rows = missing.clone();
            let pairs = py
                .detach(move || {
                    lefts
                        .storage()
                        .iter_rows()
                        .zip(rights.storage().iter_rows())
                        .enumerate()
                        .map(|(row, (left, right))| {
                            if missing_rows.as_ref().is_some_and(|mask| mask[row]) {
                                return Ok((None, CoordinateAxes::XY));
                            }
                            let left = lefts.prepared_row(row, left);
                            let right = rights.prepared_row(row, right);
                            let common = crate::geometry::common_axes(left.shape(), right.shape());
                            metric_nearest_points(&model, &left, &right).map(|pair| (pair, common))
                        })
                        .collect_rows()
                })
                .map_err(rows_err)?;
            return Ok(crate::py::support::nearest_point_columns_masked(
                pairs,
                self.frame.clone(),
                missing,
            ));
        }
        Err(expected_geometry_or_array())
    }

    pub(crate) fn delaunay_triangles_impl(
        &self,
        py: Python<'_>,
    ) -> PyResult<crate::py::vectors::Groups> {
        // Mixed-axis rows cannot land in one packed coords column (`from_points`
        // assumes axis-homogeneity); per-shape `delaunay_triangles` preserves
        // each row's ordinate layout via `carry_each`.
        if self.uniform_axes().is_none() {
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
    ) -> PyResult<crate::py::vectors::Groups> {
        let len = self.storage().len();
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
        // Unrefined Z/M rows preserve their axes while refined rows are XY, so
        // a mixed-axis batch uses the shape collector. Pure-XY batches retain
        // the packed triangle builder.
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

    pub(crate) fn polygon_triangles_impl(
        &self,
        py: Python<'_>,
    ) -> PyResult<crate::py::vectors::Groups> {
        self.flat_map_shapes_groups_budgeted(py, "triangulate", "method", |_, shape, budget| {
            shape.polygon_triangles_budgeted(budget)
        })
    }

    pub(crate) fn sample_points_impl(
        &self,
        count: &Bound<'_, PyAny>,
        seed: &Bound<'_, PyAny>,
    ) -> PyResult<crate::py::vectors::Groups> {
        let len = self.storage().len();
        let count = I64Param::parse(count, "count", len)?;
        count
            .try_validate(|value| non_negative_int("sample_points", "count", value).map(|_| ()))?;
        let seed = I64Param::parse(seed, "seed", len)?;
        let mut budget = crate::geometry::ExpansionBudget::new("sample_points", "count");
        for (row, missing) in (0..len).map(|row| (row, self.is_row_missing(row))) {
            if !missing {
                budget.add(usize::try_from(count.get(row)).expect("validated non-negative"))?;
            }
        }
        let mut shapes = Vec::with_capacity(budget.used());
        let mut offsets = vec![0_i64];
        for (row, (is_missing, shape)) in self.masked_shape_rows().enumerate() {
            // An empty row yields an empty group rather than aborting: the
            // columnar contract is that one bad row never fails the batch. The
            // SCALAR surface still raises, per the ergonomic/columnar split.
            if !is_missing && !shape.is_empty() {
                let count = usize::try_from(count.get(row)).expect("validated non-negative");
                // One rule: a geometry seeded with `s` always draws
                // `row_sample_seed(s, 0)`. A single seed spreads across rows by
                // index; per-row seeds are each that row's OWN seed, so each
                // draws the stream a scalar with that seed draws. Both keep
                // rows independent AND agree with the scalar surface — the
                // per-element lane used the raw seed and so disagreed.
                let seed = match &seed {
                    I64Param::Scalar(value) => row_sample_seed(*value as u64, row),
                    I64Param::PerElement(values) => row_sample_seed(values[row] as u64, 0),
                };
                shapes.extend(
                    shape
                        .sample_points(count, seed)
                        .map_err(|error| note_array_row(error.into(), row))?
                        .into_iter()
                        .map(Shape::Point),
                );
            }
            offsets.push(shapes.len() as i64);
        }
        crate::py::vectors::Groups::from_geometry_flat(
            Self::from_shapes(shapes, self.frame.clone()),
            offsets,
        )
    }

    pub(crate) fn voronoi_polygons_impl(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<crate::py::vectors::Groups> {
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
    ) -> PyResult<crate::py::vectors::Groups> {
        let boundary = owned_voronoi_boundary(py, clip, &self.frame, "voronoi_edges")?;
        self.flat_map_shapes_groups_budgeted(
            py,
            "voronoi_edges",
            "sites/clip topology",
            move |_, shape, budget| {
                shape.voronoi_edges_budgeted(tolerance, boundary.as_borrowed(), budget)
            },
        )
    }

    /// Per-row polygonize: each input geometry's own linework → its polygons,
    /// one group per input (`= scalar polygonize per row`). Pool raw linework
    /// with the free function ``polygonize`` on an iterable; passing a GeometryArray
    /// there is rejected so the two meanings cannot be confused.
    pub(crate) fn polygonize_impl(&self, py: Python<'_>) -> PyResult<crate::py::vectors::Groups> {
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
            let storage = Arc::clone(self.storage_arc());
            let missing = self.missing().cloned();
            py.detach(move || {
                let mut shapes = Vec::new();
                for (row, line) in storage.iter_shapes().enumerate() {
                    if missing.as_ref().is_some_and(|mask| mask[row]) {
                        continue;
                    }
                    shapes.extend(line.split(&splitter_shape, tolerance)?);
                }
                Ok::<_, Error>(shapes)
            })?
        } else if let Some(splitters) = exact_geometry_array(splitter) {
            let (lines, cutters) = paired_arrays(self, splitters, "split")?;
            let line_missing = self.missing().cloned();
            let cutter_missing = splitters.missing();
            py.detach(move || {
                let mut shapes = Vec::new();
                for (row, (line, cutter)) in
                    lines.iter_shapes().zip(cutters.iter_shapes()).enumerate()
                {
                    if line_missing.as_ref().is_some_and(|mask| mask[row])
                        || cutter_missing.as_ref().is_some_and(|mask| mask[row])
                    {
                        continue;
                    }
                    shapes.extend(line.split(&cutter, tolerance)?);
                }
                Ok::<_, Error>(shapes)
            })?
        } else {
            return Err(expected_geometry_or_array());
        };
        Ok(Self::from_shapes(shapes, self.frame.clone()))
    }

    pub(crate) fn extremes_impl(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let constructor = crate::py::support::extreme_points_type(py)?;
        // One vertex scan per present row feeds all four direction columns;
        // empty rows degrade to an empty point per column (the geometry-lane
        // sentinel) and missing rows stay missing via the scatter machinery.
        let mut columns: [Vec<Shape>; 4] = std::array::from_fn(|_| Vec::new());
        for (_, shape) in self.present_shape_rows() {
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
        let arrays = columns.map(|shapes| {
            self.scatter_present_result(Self::from_shapes(shapes, self.frame.clone()))
        });
        Ok(constructor.call1(PyTuple::new(py, arrays)?)?.unbind())
    }

    pub(crate) fn spatial_key_impl(
        &self,
        py: Python<'_>,
        curve: crate::py::support::SpatialCurve,
        level: i64,
        bounds: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let kind = curves::CurveKind::from(curve);
        crate::py::numpy::uint64_array(
            py,
            self.curve_keys(level, bounds, kind.operation_name(), kind)?,
        )
    }

    pub(crate) fn subdivide_rows(
        &self,
        py: Python<'_>,
        max_vertices: i64,
    ) -> PyResult<crate::py::vectors::Groups> {
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
mod voronoi_budget_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::Point;

    #[test]
    fn voronoi_array_budget_is_cumulative_across_rows() {
        crate::test_support::initialize_python();
        let visits = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&visits);
        let array = PyGeometryArray::from_shapes(
            vec![
                Shape::Point(Point::new_unchecked_xy(0.0, 0.0)),
                Shape::Point(Point::new_unchecked_xy(1.0, 0.0)),
            ],
            Frame::new(None, None).unwrap(),
        );
        let result = Python::attach(|py| {
            array.flat_map_shapes_groups_budgeted(
                py,
                "voronoi_polygons",
                "sites/clip topology",
                move |_, _, budget| {
                    observed.fetch_add(1, Ordering::Relaxed);
                    budget.add(crate::geometry::GENERATED_ITEM_LIMIT / 2 + 1)?;
                    Ok(Vec::new())
                },
            )
        });
        assert!(result.is_err());
        assert_eq!(visits.load(Ordering::Relaxed), 2);
    }
}
