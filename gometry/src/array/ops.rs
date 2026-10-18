//! Internal `GeometryArray` operation helpers.
//!
//! Free `#[pyfunction]` surfaces call these instead of exposing thin method
//! delegates on `GeometryArray`.

use pyo3::types::{PyBytes, PyTuple};

use crate::array::{
    Arc, ArrayRow, Bound, CollectRows as _, DefaultedF64Input, Error, F64Param, Frame,
    InvalidGeometryError, Py, PyAny, PyAnyMethods as _, PyErr, PyGeometryArray, PyResult,
    PyValidationReport, Python, RepairMethod, Result, Shape, VoronoiClipInput, VoronoiOutput,
    bool_array, cdt_refinement_values, exact_geometry, exact_geometry_array,
    expected_geometry_or_array, geometry, io, owned_voronoi_boundary, pair_packed_equals_exact,
    parse_precision, require_geojson_crs, rows_err, validate_equals_exact_tolerance,
    validate_subdivide_max_vertices,
};
use crate::geometry::publication::{PublicationRows, publish_split_groups};
use crate::geometry::storage;
use crate::geometry::storage::builder::concat_packed_line_columns;
use crate::geometry::storage::{GeometryArrayStorage, PackedStorage, ValidityMask};
use crate::py::support;
use crate::py::vectors::Groups;

fn split_paired_rows(
    lines: &PyGeometryArray,
    cutters: &PyGeometryArray,
    mut split: impl FnMut(&Shape, &Shape) -> Result<Vec<Shape>>,
) -> std::result::Result<crate::CsrBuilder<Shape>, (usize, Error)> {
    let mut builder = crate::CsrBuilder::<Shape>::with_capacity(0, lines.len());
    for (line, cutter) in lines
        .row_operation()
        .rows_with_missing(|| ())
        .zip(cutters.row_operation().rows_with_missing(|| ()))
    {
        let (row, values) = match (line, cutter) {
            (ArrayRow::Present(line), ArrayRow::Present(cutter)) => {
                let logical = line.logical();
                let values = line
                    .row()
                    .try_with_shape(|line| {
                        cutter.row().try_with_shape(|cutter| split(line, cutter))
                    })
                    .map_err(|error| (logical, error))?;
                (logical, values)
            },
            (line, _) => {
                let logical = match line {
                    ArrayRow::Present(line) => line.logical(),
                    ArrayRow::Missing { logical, .. } => logical,
                };
                (logical, Vec::new())
            },
        };
        builder.push_row(values).map_err(|error| (row, error))?;
    }
    Ok(builder)
}

fn publish_split_group_builder(
    builder: crate::CsrBuilder<Shape>,
    frame: Frame,
    missing: Option<ValidityMask>,
) -> PyResult<Groups> {
    let (shapes, offsets) = builder.finish();
    publish_split_groups(shapes, &offsets, frame, missing).map_err(PyErr::from)
}

fn split_array_groups(
    py: Python<'_>,
    lines: &PyGeometryArray,
    frame: Frame,
    missing: Option<ValidityMask>,
    mut split: impl FnMut(&Shape) -> Result<Vec<Shape>> + Send + pyo3::marker::Ungil,
) -> PyResult<Groups> {
    let array = lines.clone();
    let builder = py
        .detach(move || {
            let mut builder = crate::CsrBuilder::<Shape>::with_capacity(0, array.len());
            for row in array.row_operation().rows_with_missing(Vec::new) {
                let (logical, shapes) = match row {
                    ArrayRow::Missing { logical, value } => (logical, value),
                    ArrayRow::Present(row) => {
                        let logical = row.logical();
                        let shapes = row
                            .row()
                            .try_with_shape(|shape| split(shape))
                            .map_err(|error| (logical, error))?;
                        (logical, shapes)
                    },
                };
                builder.push_row(shapes).map_err(|error| (logical, error))?;
            }
            Ok::<_, (usize, Error)>(builder)
        })
        .map_err(rows_err)?;
    publish_split_group_builder(builder, frame, missing)
}

fn publish_triangle_shapes_for_domain(
    shapes: Vec<Shape>,
    domain: crate::geometry::TopologyDomain,
    metric: crate::dispatch::MetricCtx<'_>,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<Vec<Shape>> {
    match domain {
        crate::geometry::TopologyDomain::Cartesian => Ok(shapes),
        crate::geometry::TopologyDomain::Geographic(frame) => {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic triangulation requires the operation's CRS metric",
                    )
                })
            };
            let source_context = geometry::SourceArcContext::new(domain, Some(&metric_access));
            geometry::publish_raw_computed_shapes_with_budget_and_context(
                &shapes,
                frame,
                &source_context,
                budget,
            )
        },
    }
}

fn publish_triangle_vertices_for_domain(
    vertices: Vec<crate::geometry::Point>,
    domain: crate::geometry::TopologyDomain,
    metric: crate::dispatch::MetricCtx<'_>,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<crate::geometry::PublishedTriangleVertices> {
    match domain {
        crate::geometry::TopologyDomain::Cartesian => {
            crate::geometry::PublishedTriangleVertices::uniform(vertices)
        },
        crate::geometry::TopologyDomain::Geographic(frame) => {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic triangulation requires the operation's CRS metric",
                    )
                })
            };
            let source_context = geometry::SourceArcContext::new(domain, Some(&metric_access));
            geometry::publish_triangle_vertex_stream_budgeted(
                &vertices,
                frame,
                &source_context,
                budget,
            )
        },
    }
}

fn publish_mesh_publication_for_domain(
    publication: crate::geometry::PreparedMeshPublication,
    domain: crate::geometry::TopologyDomain,
    metric: crate::dispatch::MetricCtx<'_>,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<crate::geometry::PublishedTriangleVertices> {
    let metric_access = || {
        metric.geographic_chart_metric()?.ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "geographic triangulation requires the operation's CRS metric",
            )
        })
    };
    let source_context = geometry::SourceArcContext::new(domain, Some(&metric_access));
    geometry::publish_mesh_publication_for_domain(publication, domain, &source_context, budget)
}

fn publish_earcut_publication_for_domain(
    publication: crate::geometry::PreparedMeshPublication,
    domain: crate::geometry::TopologyDomain,
    metric: crate::dispatch::MetricCtx<'_>,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<crate::geometry::PublishedTriangleVertices> {
    let metric_access = || {
        metric.geographic_chart_metric()?.ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "geographic triangulation requires the operation's CRS metric",
            )
        })
    };
    let source_context = geometry::SourceArcContext::new(domain, Some(&metric_access));
    geometry::publish_earcut_mesh_publication_for_domain(
        publication,
        domain,
        &source_context,
        budget,
    )
}

fn geographic_mesh_groups_budgeted(
    array: &PyGeometryArray,
    py: Python<'_>,
    operation: &'static str,
    parameter: &'static str,
    mut transform: impl FnMut(
        usize,
        &Shape,
        &mut crate::geometry::ExpansionBudget,
    ) -> Result<crate::geometry::PublishedTriangleVertices>
    + Send
    + pyo3::marker::Ungil,
) -> PyResult<Groups> {
    let source = array.clone();
    let frame = array.frame.clone();
    let missing = array.missing().cloned();
    let rows = py
        .detach(move || {
            let mut rows = Vec::with_capacity(source.len());
            for item in source.row_operation().rows_with_missing(|| ()) {
                match item {
                    ArrayRow::Missing { .. } => rows.push(None),
                    ArrayRow::Present(row) => {
                        let logical = row.logical();
                        let mut budget =
                            crate::geometry::ExpansionBudget::new(operation, parameter);
                        let published = row
                            .row()
                            .try_with_shape(|shape| transform(logical, shape, &mut budget))
                            .map_err(|error| (logical, error))?;
                        rows.push(Some(published));
                    },
                }
            }
            Ok::<_, (usize, crate::error::Error)>(rows)
        })
        .map_err(rows_err)?;
    let finished =
        py.detach(move || geometry::finish_mesh_publication_groups(rows, frame, missing))?;
    Groups::from_finished(finished).map_err(Into::into)
}

impl PyGeometryArray {
    pub(crate) fn to_wkt_impl(
        &self,
        py: Python<'_>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<String>> {
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
                        io::to_wkt_with_srid(&shape, crs.as_deref(), include_srid)
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
        let len = self.len();
        // An array CRS is uniform across present rows. Resolve it once, but
        // retain the old all-missing behavior by skipping validation when no
        // row can emit a payload. Attach the same first-present row note as
        // the row loop when resolution itself fails.
        let has_present = len != 0 && self.missing().is_none_or(|mask| mask.present_count() != 0);
        let srid = if include_srid && has_present {
            match crate::io::extended_srid_code(self.crs_str(), true, "EWKB") {
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
        let mut output = Vec::with_capacity(len);
        let mut rows = self.row_operation().rows_with_missing(|| ()).enumerate();
        while rows.len() != 0 {
            let (buffer, ends) = py
                .detach(
                    || -> Result<(Vec<u8>, Vec<usize>), (usize, crate::error::Error)> {
                        let mut buffer: Vec<u8> = Vec::new();
                        let mut ends: Vec<usize> = Vec::new();
                        for (row, logical_row) in rows.by_ref() {
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
                            ends.push(buffer.len());
                            if buffer.len() >= FLUSH_BYTES {
                                break;
                            }
                        }
                        Ok((buffer, ends))
                    },
                )
                .map_err(rows_err)?;
            let mut start = 0;
            for end in ends {
                let bytes = PyBytes::new(
                    py,
                    buffer
                        .get(start..end)
                        .expect("WKB chunk ends match the accumulated byte buffer"),
                )
                .unbind();
                start = end;
                output.push(bytes);
            }
        }
        Ok(output)
    }

    pub(crate) fn to_geojson_impl(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        require_geojson_crs(self.crs_str())?;
        // O(1) axis probe off the packed columns instead of materializing every
        // row's `Shape`. M is unrepresentable in GeoJSON: refuse it by default
        // (no silent loss).
        if self.has_m() {
            return Err(InvalidGeometryError::new_err(crate::io::GEOJSON_M_ERROR));
        }
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = support::geojson_domain_metric(self.crs_str(), &mut metric_scratch)?;
        let metric_access = move || {
            metric.geographic_chart_metric()?.ok_or_else(|| {
                crate::crs::CrsError::invalid(
                    "geographic GeoJSON source ordinates require the array CRS metric",
                )
            })
        };
        let array = self.clone();
        // GeoJSON treats CRS-free coordinates as WGS84 lon/lat too. A
        // domain-invalid row is a spec violation, not a degradable data
        // condition, so it fails the batch (like the M-ordinate refusal above).
        py.detach(move || array.geojson_rows(domain, &metric_access))
            .map_err(rows_err)
    }

    fn geojson_rows(
        &self,
        domain: geometry::TopologyDomain,
        metric_access: &crate::geometry::SourceArcMetricAccess<'_, '_>,
    ) -> std::result::Result<Vec<String>, (usize, crate::error::Error)> {
        let mut output = Vec::with_capacity(self.len());
        for row in self.row_operation().rows_with_missing(String::new) {
            let value = match row {
                ArrayRow::Missing { value, .. } => value,
                ArrayRow::Present(row) => {
                    let logical = row.logical();
                    row.row()
                        .try_with_shape(|shape| {
                            io::to_geojson_string_with_metric(shape, domain, metric_access)
                        })
                        .map_err(|error| (logical, error))?
                },
            };
            output.push(value);
        }
        Ok(output)
    }

    pub(crate) fn validate_impl(
        &self,
        py: Python<'_>,
    ) -> PyResult<Vec<Option<PyValidationReport>>> {
        let domain = geometry::topology_domain(&self.frame)?;
        // Validating every row is the same native work `repair` detaches.
        py.detach(|| {
            let mut reports = Vec::with_capacity(self.len());
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
                reports.push(report);
            }
            Ok(reports)
        })
    }

    pub(crate) fn repair_impl(&self, py: Python<'_>, method: RepairMethod) -> PyResult<Self> {
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        let metric_access = || {
            metric.geographic_chart_metric()?.ok_or_else(|| {
                crate::crs::CrsError::invalid(
                    "geographic repair source ordinates require the operation's CRS metric",
                )
            })
        };
        let repaired = py
            .detach(|| {
                let source_context =
                    crate::geometry::SourceArcContext::new(domain, Some(&metric_access));
                let mut all_present_valid = true;
                let mut shapes = Vec::with_capacity(
                    self.missing()
                        .map_or(self.len(), storage::ValidityMask::present_count),
                );
                for logical_row in self.row_operation().rows_with_missing(|| ()) {
                    let ArrayRow::Present(row) = logical_row else {
                        continue;
                    };
                    let logical = row.logical();
                    let (shape, changed) = row
                        .row()
                        .try_with_shape(|shape| {
                            Ok::<_, crate::error::Error>(
                                geometry::repair_shape_in_frame(
                                    shape,
                                    domain,
                                    method,
                                    &source_context,
                                )?
                                .map_or_else(
                                    || (shape.clone(), false),
                                    |repaired| (repaired, true),
                                ),
                            )
                        })
                        .map_err(|error| (logical, error))?;
                    all_present_valid &= !changed;
                    shapes.push(shape);
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
            let results = crate::geometry::dispatch_axes!((include_z, include_m), HAS_Z, HAS_M, {
                array_equals_exact_scalar_tolerance::<HAS_Z, HAS_M>(py, self, other, scalar_tol)
            })?;
            return bool_array(py, results);
        }
        let results = crate::geometry::dispatch_axes!((include_z, include_m), HAS_Z, HAS_M, {
            array_equals_exact_row_tolerance::<HAS_Z, HAS_M>(py, self, other, &tolerance)
        })?;
        bool_array(py, results)
    }

    pub(crate) fn delaunay_triangles_impl(&self, py: Python<'_>) -> PyResult<Groups> {
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        if matches!(domain, crate::geometry::TopologyDomain::Geographic(_)) {
            return geographic_mesh_groups_budgeted(
                self,
                py,
                "triangulate",
                "method",
                move |_, shape, budget| {
                    let publication =
                        shape.delaunay_mesh_publication_in_domain_budgeted(domain, budget)?;
                    publish_mesh_publication_for_domain(publication, domain, metric, budget)
                },
            );
        }
        if self.uniform_axes().is_none() {
            return self.flat_map_shapes_groups_budgeted(
                py,
                "triangulate",
                "method",
                move |_, shape, budget| shape.delaunay_triangles_budgeted(budget),
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
            move |_, shape, budget| {
                let publication =
                    shape.delaunay_mesh_publication_in_domain_budgeted(domain, budget)?;
                publish_mesh_publication_for_domain(publication, domain, metric, budget)
            },
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
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        if matches!(domain, crate::geometry::TopologyDomain::Geographic(_)) {
            let array = self.clone();
            let mut writer =
                geometry::CdtGroupsDrawingWriter::new(self.len(), self.missing().cloned())?;
            let frame = self.frame.clone();
            let receipt = py
                .detach(move || {
                    for row in array.row_operation().rows_with_missing(Vec::<Shape>::new) {
                        match row {
                            ArrayRow::Missing { logical, .. } => {
                                writer
                                    .append_row(logical, None)
                                    .map_err(|error| (logical, error))?;
                            },
                            ArrayRow::Present(row) => {
                                let logical = row.logical();
                                let mut budget = crate::geometry::ExpansionBudget::new(
                                    "triangulate",
                                    "min_angle/max_area",
                                );
                                let drawing = row
                                    .row()
                                    .try_with_shape(|shape| {
                                        geometry::prepare_geographic_cdt_drawing(
                                            shape,
                                            *refinements
                                                .get(logical)
                                                .expect("CDT refinement values match input rows"),
                                            domain,
                                            metric,
                                            &mut budget,
                                        )
                                    })
                                    .map_err(|error| (logical, error))?;
                                writer
                                    .append_row(logical, Some(drawing))
                                    .map_err(|error| (logical, error))?;
                            },
                        }
                    }
                    let final_row = array.len().saturating_sub(1);
                    writer.finish(frame).map_err(|error| (final_row, error))
                })
                .map_err(rows_err)?;
            return Ok(Groups::from_finished(receipt)?);
        }
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
                    let triangles = shape.constrained_delaunay_triangles_in_domain_budgeted(
                        *refinements
                            .get(row)
                            .expect("refinement values match the admitted array rows"),
                        domain,
                        budget,
                    )?;
                    publish_triangle_shapes_for_domain(triangles, domain, metric, budget)
                },
            );
        }
        self.flat_map_packed_triangles_groups_budgeted(
            py,
            "triangulate",
            "min_angle/max_area",
            move |row, shape, budget| {
                let vertices = shape.constrained_delaunay_vertices_in_domain_budgeted(
                    *refinements
                        .get(row)
                        .expect("refinement values match the admitted array rows"),
                    domain,
                    budget,
                )?;
                publish_triangle_vertices_for_domain(vertices, domain, metric, budget)
            },
        )
    }

    pub(crate) fn polygon_triangles_impl(&self, py: Python<'_>) -> PyResult<Groups> {
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        if matches!(domain, crate::geometry::TopologyDomain::Geographic(_)) {
            return geographic_mesh_groups_budgeted(
                self,
                py,
                "triangulate",
                "method",
                move |_, shape, budget| {
                    let publication = shape.earcut_mesh_publication_budgeted(budget)?;
                    publish_earcut_publication_for_domain(publication, domain, metric, budget)
                },
            );
        }
        if self.earcut_rows_pack()? {
            return self.flat_map_packed_triangles_groups_budgeted(
                py,
                "triangulate",
                "method",
                move |_, shape, budget| {
                    let vertices = shape.polygon_triangle_vertices_budgeted(budget)?;
                    crate::geometry::PublishedTriangleVertices::uniform(vertices)
                },
            );
        }
        self.flat_map_shapes_groups_budgeted(
            py,
            "triangulate",
            "method",
            move |_, shape, budget| shape.polygon_triangles_budgeted(budget),
        )
    }

    fn earcut_rows_pack(&self) -> crate::error::Result<bool> {
        Ok(match self.storage() {
            GeometryArrayStorage::Packed(PackedStorage::Polygons { .. }) => true,
            GeometryArrayStorage::Packed(_) => false,
            GeometryArrayStorage::Mixed(_) => {
                let mut axes = None;
                for row in self.row_operation().present_rows() {
                    let Some(row) = row.row().with_shape(Shape::earcut_packed_axes)? else {
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

    pub(crate) fn voronoi_cells_impl(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<Groups> {
        let boundary = owned_voronoi_boundary(py, clip, &self.frame, "voronoi_cells")?;
        let geographic_frame = match crate::geometry::topology_domain(&self.frame)? {
            crate::geometry::TopologyDomain::Geographic(frame) => Some(frame),
            crate::geometry::TopologyDomain::Cartesian => None,
        };
        if let Some(geographic_frame) = geographic_frame {
            let array = self.clone();
            let frame = self.frame.clone();
            let missing = self.missing().cloned();
            let row_count = array.len();
            let finished = py
                .detach(move || {
                    let mut writer =
                        geometry::publication::PublicationRows::with_groups_capacity(row_count);
                    for row in array.row_operation().rows_with_missing(|| ()) {
                        let logical = match row {
                            ArrayRow::Missing { logical, .. } => logical,
                            ArrayRow::Present(row) => {
                                let logical = row.logical();
                                let mut budget = crate::geometry::ExpansionBudget::new(
                                    "voronoi_cells",
                                    "sites/clip topology",
                                );
                                row.row()
                                    .try_with_shape(|shape| {
                                        let outputs = shape
                                            .voronoi_polygon_outputs_in_frame_budgeted(
                                                tolerance,
                                                boundary.as_borrowed(),
                                                Some(geographic_frame),
                                                &mut budget,
                                            )?;
                                        writer.push_voronoi_polygon_outputs(
                                            outputs,
                                            geographic_frame,
                                            &mut budget,
                                        )
                                    })
                                    .map_err(|error| (logical, error))?;
                                logical
                            },
                        };
                        writer
                            .end_group(
                                missing
                                    .as_ref()
                                    .is_some_and(|mask| mask.is_missing(logical)),
                            )
                            .map_err(|error| (logical, error))?;
                    }
                    writer
                        .finish_groups_geometry(frame, missing)
                        .map_err(|error| (row_count.saturating_sub(1), error))
                })
                .map_err(rows_err)?;
            return Ok(Groups::from_finished(finished)?);
        }
        self.flat_map_shapes_groups_budgeted(
            py,
            "voronoi_cells",
            "sites/clip topology",
            move |_, shape, budget| {
                let outputs = shape.voronoi_polygon_outputs_in_frame_budgeted(
                    tolerance,
                    boundary.as_borrowed(),
                    geographic_frame,
                    budget,
                )?;
                let published: geometry::PublishedVoronoiPolygonOutputs =
                    crate::geometry::publish_voronoi_polygon_outputs(
                        outputs,
                        geographic_frame,
                        budget,
                    )?;
                let (shapes, _receipt): (_, geometry::VoronoiReceiptHold) = published.into_parts();
                Ok(shapes)
            },
        )
    }

    pub(crate) fn voronoi_edge_lines_impl(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<Groups> {
        let boundary = owned_voronoi_boundary(py, clip, &self.frame, "voronoi_edge_lines")?;
        let geographic_frame = match geometry::topology_domain(&self.frame)? {
            geometry::TopologyDomain::Geographic(frame) => Some(frame),
            geometry::TopologyDomain::Cartesian => None,
        };
        if let Some(geographic_frame) = geographic_frame {
            let array = self.clone();
            let frame = self.frame.clone();
            let missing = self.missing().cloned();
            let row_count = array.len();
            let finished = py
                .detach(move || {
                    let mut writer =
                        geometry::publication::PublicationRows::with_groups_capacity(row_count);
                    for row in array.row_operation().rows_with_missing(|| ()) {
                        let logical = match row {
                            ArrayRow::Missing { logical, .. } => logical,
                            ArrayRow::Present(row) => {
                                let logical = row.logical();
                                let mut budget = geometry::ExpansionBudget::new(
                                    "voronoi_edge_lines",
                                    "sites/clip topology",
                                );
                                row.row()
                                    .try_with_shape(|shape| {
                                        let output = shape.voronoi_edge_column_in_frame_budgeted(
                                            tolerance,
                                            boundary.as_borrowed(),
                                            Some(geographic_frame),
                                            &mut budget,
                                        )?;
                                        writer.push_voronoi_edge_column(
                                            output,
                                            geographic_frame,
                                            &mut budget,
                                        )
                                    })
                                    .map_err(|error| (logical, error))?;
                                logical
                            },
                        };
                        writer
                            .end_group(
                                missing
                                    .as_ref()
                                    .is_some_and(|mask| mask.is_missing(logical)),
                            )
                            .map_err(|error| (logical, error))?;
                    }
                    writer
                        .finish_groups_geometry(frame, missing)
                        .map_err(|error| (row_count.saturating_sub(1), error))
                })
                .map_err(rows_err)?;
            return Ok(Groups::from_finished(finished)?);
        }
        let array = self.clone();
        let frame = self.frame.clone();
        let packed = py
            .detach(move || {
                let mut columns = Vec::new();
                let mut receipts = Vec::new();
                let mut group_offsets = Vec::with_capacity(array.len().wrapping_add(1));
                group_offsets.push(0_i64);
                for logical_row in array.row_operation().rows_with_missing(|| ()) {
                    let (row, n_lines) = match logical_row {
                        ArrayRow::Missing { logical, .. } => (logical, 0_usize),
                        ArrayRow::Present(row) => {
                            let logical = row.logical();
                            let (coords, offsets) = row
                                .row()
                                .try_with_shape(|shape| {
                                    let mut budget = crate::geometry::ExpansionBudget::new(
                                        "voronoi_edge_lines",
                                        "sites/clip topology",
                                    );
                                    let output = shape.voronoi_edge_column_in_frame_budgeted(
                                        tolerance,
                                        boundary.as_borrowed(),
                                        geographic_frame,
                                        &mut budget,
                                    )?;
                                    let published: geometry::PublishedVoronoiEdgeColumn =
                                        geometry::publish_voronoi_edge_column_with_budget(
                                            output,
                                            geographic_frame,
                                            &mut budget,
                                        )?;
                                    let (coords, offsets, receipt): (
                                        _,
                                        _,
                                        geometry::VoronoiReceiptHold,
                                    ) = published.into_parts();
                                    receipts.push(receipt);
                                    Ok((coords, offsets))
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
                Ok((columns, group_offsets, receipts))
            })
            .map_err(rows_err)?;
        let (columns, group_offsets, receipts) = packed;
        let (coords, line_offsets) = py.detach(move || concat_packed_line_columns(columns))?;
        let output = Groups::from_packed_line_groups(
            frame,
            coords,
            line_offsets,
            group_offsets,
            self.missing().cloned(),
        )?;
        drop(receipts);
        Ok(output)
    }

    pub(crate) fn voronoi_impl(
        &self,
        py: Python<'_>,
        output: VoronoiOutput,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<Groups> {
        match output {
            VoronoiOutput::Polygons => self.voronoi_cells_impl(py, tolerance, clip),
            VoronoiOutput::Edges => self.voronoi_edge_lines_impl(py, tolerance, clip),
        }
    }

    /// Per-row polygonize: each input geometry's own linework → its polygons,
    /// one group per input (`= scalar polygonize per row`). Combining rows would
    /// discard that row-to-result ownership; use ``polygonize_all`` when edges
    /// from different rows should close one another.
    pub(crate) fn polygonize_impl(&self, py: Python<'_>) -> PyResult<Groups> {
        let output_frame = self.frame.clone();
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        if domain.is_geographic() {
            let array = self.clone();
            let missing = self.missing().cloned();
            let groups = py
                .detach(move || {
                    let mut publication = PublicationRows::with_groups_capacity(array.len());
                    for row in array
                        .row_operation()
                        .rows_with_missing(Vec::<Shape>::new)
                    {
                        match row {
                            ArrayRow::Missing { .. } => publication.end_group(true).map_err(|error| {
                                (array.len(), error)
                            })?,
                            ArrayRow::Present(row) => {
                                let logical = row.logical();
                                let metric_access = || {
                                    metric.geographic_chart_metric()?.ok_or_else(|| {
                                        crate::crs::CrsError::invalid(
                                            "geographic polygonize source ordinates require the operation's CRS metric",
                                        )
                                    })
                                };
                                let context = crate::geometry::SourceArcContext::new(
                                    domain,
                                    Some(&metric_access),
                                );
                                row
                                    .row()
                                    .try_with_shape(|shape| {
                                        Shape::append_polygonize_rows(
                                            &[shape],
                                            false,
                                            &context,
                                            &output_frame,
                                            Some(logical),
                                            &mut publication,
                                        )
                                    })
                                    .map_err(|error| (logical, error))?;
                                publication
                                    .end_group(false)
                                    .map_err(|error| (logical, error))?;
                            },
                        }
                    }
                    publication
                        .finish_groups(output_frame, missing)
                        .map_err(|error| (array.len(), error))
                })
                .map_err(rows_err)?;
            drop(metric_scratch);
            return Ok(groups);
        }
        self.flat_map_shapes_groups(py, move |shape| {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic polygonize source ordinates require the operation's CRS metric",
                    )
                })
            };
            shape.polygonize_with_publication_frame(
                false,
                &crate::geometry::SourceArcContext::new(domain, Some(&metric_access)),
                output_frame.clone(),
            )
        })
    }

    pub(crate) fn line_merge_impl(&self, py: Python<'_>) -> PyResult<Self> {
        if let Some(identity) = self.packed_lines_identity() {
            return Ok(identity);
        }
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        self.map_shapes_detached(py, move |shape| {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic line_merge source ordinates require the operation's CRS metric",
                    )
                })
            };
            shape.line_merge_with_context(&crate::geometry::SourceArcContext::new(
                domain,
                Some(&metric_access),
            ))
        })
    }

    /// `tolerance` is already validated non-negative at the free-function boundary.
    pub(crate) fn split(
        &self,
        py: Python<'_>,
        splitter: &Bound<'_, PyAny>,
        tolerance: f64,
    ) -> PyResult<Groups> {
        if let Some(splitter) = exact_geometry(splitter) {
            Frame::compatible_parts(
                self.crs_ref(),
                self.epoch(),
                splitter.crs_ref(),
                splitter.epoch(),
                "split",
            )?;
            let splitter_shape = Arc::clone(&splitter.shape);
            if self.has_z() || self.has_m() {
                let mut metric_scratch = crate::dispatch::MetricScratch::default();
                let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
                    .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
                if domain.is_geographic() {
                    return split_array_groups(
                        py,
                        self,
                        self.frame.clone(),
                        self.missing().cloned(),
                        move |line| {
                            let metric_access = || {
                                metric.geographic_chart_metric()?.ok_or_else(|| {
                                crate::crs::CrsError::invalid(
                                    "geographic split source ordinates require the operation's CRS metric",
                                )
                            })
                            };
                            let context =
                                geometry::SourceArcContext::new(domain, Some(&metric_access));
                            if geometry::source_arc_ordinates_eligible(line.axes(), domain) {
                                line.split_with_source_arcs(&splitter_shape, tolerance, &context)
                            } else {
                                line.split(&splitter_shape, tolerance)
                            }
                        },
                    );
                }
            }
            return split_array_groups(
                py,
                self,
                self.frame.clone(),
                self.missing().cloned(),
                move |line| line.split(&splitter_shape, tolerance),
            );
        }
        if let Some(splitters) = exact_geometry_array(splitter) {
            crate::broadcast::validate_paired_arrays(self, splitters, "split")?;
            let missing = ValidityMask::from_missing_iter(
                self.len(),
                (0..self.len())
                    .map(|row| self.is_row_missing(row) || splitters.is_row_missing(row)),
            );
            let lines = self.clone();
            let cutters = splitters.clone();
            let builder = if self.has_z() || self.has_m() {
                let mut metric_scratch = crate::dispatch::MetricScratch::default();
                let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
                    .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
                if domain.is_geographic() {
                    py.detach(move || {
                        let metric_access = || {
                            metric.geographic_chart_metric()?.ok_or_else(|| {
                                crate::crs::CrsError::invalid(
                                    "geographic split source ordinates require the operation's CRS metric",
                                )
                            })
                        };
                        let context =
                            geometry::SourceArcContext::new(domain, Some(&metric_access));
                        split_paired_rows(&lines, &cutters, |line, cutter| {
                            if geometry::source_arc_ordinates_eligible(line.axes(), domain) {
                                line.split_with_source_arcs(cutter, tolerance, &context)
                            } else {
                                line.split(cutter, tolerance)
                            }
                        })
                    })
                    .map_err(rows_err)?
                } else {
                    py.detach(move || {
                        split_paired_rows(&lines, &cutters, |line, cutter| {
                            line.split(cutter, tolerance)
                        })
                    })
                    .map_err(rows_err)?
                }
            } else {
                py.detach(move || {
                    split_paired_rows(&lines, &cutters, |line, cutter| {
                        line.split(cutter, tolerance)
                    })
                })
                .map_err(rows_err)?
            };
            return publish_split_group_builder(builder, self.frame.clone(), missing);
        }
        Err(expected_geometry_or_array())
    }

    pub(crate) fn extremes_impl(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let constructor = support::extreme_points_type(py)?;
        // One vertex scan per present row feeds all four direction columns;
        // empty rows degrade to an empty point per column (the geometry-lane
        // sentinel) and missing rows stay missing via the scatter machinery.
        let present_count = self
            .missing()
            .map_or(self.len(), storage::ValidityMask::present_count);
        let mut columns: [Vec<Shape>; 4] =
            std::array::from_fn(|_| Vec::with_capacity(present_count));
        for row in self.row_operation().present_rows() {
            match row.row().with_shape(Shape::extremes).map_err(PyErr::from)? {
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
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        let metric_access = move || {
            metric.geographic_chart_metric()?.ok_or_else(|| {
                crate::crs::CrsError::invalid(
                    "geographic subdivision source ordinates require the operation's CRS metric",
                )
            })
        };
        // Per-row groups: each input geometry's subdivided parts stay grouped
        // under their source row. `iter_shapes` gives packed rows CSR views with
        // no Mixed boxing, so the group build is as cheap as the flat path was.
        self.flat_map_shapes_groups(py, move |shape| {
            shape.subdivide_with_context(
                max_vertices,
                false,
                &crate::geometry::SourceArcContext::new(domain, Some(&metric_access)),
            )
        })
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
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
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
                    "voronoi_cells",
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
