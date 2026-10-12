//! Internal geometry operation helpers.
//!
//! Free `#[pyfunction]` surfaces call these instead of exposing thin method
//! delegates on `Geometry`.

use std::sync::Arc;

use pyo3::types::{PyBytes, PyTuple};

use crate::py::classes::geometry_methods::{
    Bound, InvalidGeometryError, Py, PyAny, PyAnyMethods as _, PyGeometry, PyResult, Python, Typed,
};
use crate::py::support;
use crate::py::support::VoronoiOutput;
use crate::{
    Frame, PyGeometryArray, PyValidationReport, RepairMethod, Shape, VoronoiClipInput, geometry,
    io, parse_cdt_refinement, parse_precision, parse_sample_count, parse_sample_seed,
    require_geojson_crs, validate_subdivide_max_vertices, voronoi_edge_lines_flatten,
    voronoi_flatten,
};

fn publish_triangle_shapes_for_domain(
    shapes: Vec<Shape>,
    domain: geometry::TopologyDomain,
    metric: crate::dispatch::MetricCtx<'_>,
) -> crate::error::Result<Vec<Shape>> {
    match domain {
        geometry::TopologyDomain::Cartesian => Ok(shapes),
        geometry::TopologyDomain::Geographic(frame) => {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic triangulation requires the operation's CRS metric",
                    )
                })
            };
            let source_context = geometry::SourceArcContext::new(domain, Some(&metric_access));
            geometry::publish_raw_computed_shapes_with_context(
                &shapes,
                frame,
                "triangulate",
                &source_context,
            )
        },
    }
}

impl PyGeometry {
    pub(crate) fn parts_to_array(
        &self,
        shapes: impl IntoIterator<Item = Shape>,
    ) -> PyResult<PyGeometryArray> {
        // Shape-native sink: never stage per-part `PyGeometry` wrappers.
        Ok(PyGeometryArray::from_shapes(
            shapes.into_iter().collect(),
            self.frame.clone(),
        )?)
    }

    pub(crate) fn to_wkt_impl(
        &self,
        py: Python<'_>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<String> {
        let precision = precision.map(parse_precision).transpose()?;
        let shape = Arc::clone(&self.shape);
        let crs = self.crs_ref().cloned();
        py.detach(move || {
            let quantized = precision
                .map(|precision| shape.quantize(precision))
                .transpose()?;
            Ok(io::to_wkt_with_srid(
                quantized.as_ref().unwrap_or(&shape),
                crs.as_deref(),
                include_srid,
            )?)
        })
    }

    pub(crate) fn to_wkb_impl<'py>(
        &self,
        py: Python<'py>,
        include_srid: bool,
        precision: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let precision = precision.map(parse_precision).transpose()?;
        let shape = Arc::clone(&self.shape);
        let crs = self.crs_str().map(str::to_owned);
        let bytes = py.detach(move || {
            let quantized = precision
                .map(|precision| shape.quantize(precision))
                .transpose()?;
            let shape = quantized.as_ref().unwrap_or_else(|| shape.shape());
            io::to_wkb(shape, crs.as_deref(), include_srid)
        })?;
        Ok(PyBytes::new(py, &bytes))
    }

    pub(crate) fn to_geojson_impl(&self, py: Python<'_>) -> PyResult<String> {
        require_geojson_crs(self.crs_str())?;
        if self.shape.has_m() {
            return Err(InvalidGeometryError::new_err(crate::io::GEOJSON_M_ERROR));
        }
        // GeoJSON treats CRS-free coordinates as WGS84 lon/lat too, so the
        // public writer always selects the RFC 7946 geographic rules.
        let shape = Arc::clone(&self.shape);
        let frame = self.frame.clone();
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) =
            support::geojson_domain_metric(frame.crs_str(), &mut metric_scratch)?;
        let metric_access = move || {
            metric.geographic_chart_metric()?.ok_or_else(|| {
                crate::crs::CrsError::invalid(
                    "geographic GeoJSON source ordinates require the geometry CRS metric",
                )
            })
        };
        py.detach(move || {
            Ok(io::to_geojson_string_with_metric(
                &shape,
                domain,
                &metric_access,
            )?)
        })
    }

    pub(crate) fn validate_impl(&self, py: Python<'_>) -> PyResult<PyValidationReport> {
        let geometry = self.clone();
        let plan = self.prepared_plan();
        let domain = geometry::topology_domain(&self.frame)?;
        Ok(PyValidationReport {
            geometry,
            issue: py.detach(move || geometry::validate_data_in_frame(&plan, domain))?,
        })
    }

    pub(crate) fn repair_impl(&self, py: Python<'_>, method: RepairMethod) -> PyResult<Typed> {
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
        let plan = self.prepared_plan();
        let repaired = py.detach(move || {
            let source_context =
                crate::geometry::SourceArcContext::new(domain, Some(&metric_access));
            geometry::repair_data_in_frame(&plan, domain, method, &source_context)
        })?;
        Ok(Typed(repaired.map_or_else(
            || self.clone(),
            |shape| self.with_shape(shape),
        )))
    }

    pub(crate) fn self_intersections_impl(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        let plan = self.prepared_plan();
        let domain = geometry::topology_domain(&self.frame)?;
        let points =
            py.detach(move || geometry::self_intersections_data_in_frame(&plan, domain))?;
        self.parts_to_array(points.into_iter().map(Shape::Point))
    }

    pub(crate) fn delaunay_triangles_impl(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        // Build the packed `Polygons` layout straight from the triangulation's
        // flat vertex stream — one coords column + arithmetic CSR offsets — so a
        // 1000-triangle result skips 1000 per-triangle `Polygon`/`CoordSeq`
        // allocations and the re-pack scan (measured ~50% of the op).
        let shape = Arc::clone(&self.shape);
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        let publication = py.detach(move || {
            let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
            let publication =
                shape.delaunay_mesh_publication_in_domain_budgeted(domain, &mut budget)?;
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic triangulation requires the operation's CRS metric",
                    )
                })
            };
            let source_context = geometry::SourceArcContext::new(domain, Some(&metric_access));
            geometry::publish_mesh_publication_for_domain(
                publication,
                domain,
                &source_context,
                &mut budget,
            )
        })?;
        let vertices = publication.points.as_slice();
        if vertices.is_empty() {
            return Ok(PyGeometryArray::from_shapes(
                Vec::new(),
                self.frame.clone(),
            )?);
        }
        let axes = vertices.first().map(|point| point.axes);
        if vertices.iter().any(|point| Some(point.axes) != axes) {
            let triangles =
                Shape::triangle_shapes_from_published_rings(vertices, &publication.ring_ends)?;
            return self.parts_to_array(triangles);
        }
        PyGeometryArray::packed_triangles_with_ring_ends(
            vertices,
            &publication.ring_ends,
            self.frame.clone(),
        )
    }

    pub(crate) fn constrained_delaunay_triangles_impl(
        &self,
        py: Python<'_>,
        min_angle: Option<&Bound<'_, PyAny>>,
        max_area: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyGeometryArray> {
        let refinement = parse_cdt_refinement(min_angle, max_area)?;
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        // Refinement inserts Steiner vertices, so output is XY-only. The
        // source-site mesh plan supplies the same selected points and axes to
        // both the packed and per-shape publication paths.
        if refinement.active() || (!self.shape.has_z() && !self.shape.has_m()) {
            let shape = Arc::clone(&self.shape);
            let vertices = py.detach(move || {
                shape.constrained_delaunay_vertices_in_domain(refinement, domain)
            })?;
            return if vertices.is_empty() {
                Ok(PyGeometryArray::from_shapes(
                    Vec::new(),
                    self.frame.clone(),
                )?)
            } else {
                PyGeometryArray::packed_triangles(&vertices, self.frame.clone())
            };
        }
        let shape = Arc::clone(&self.shape);
        let triangles =
            py.detach(move || shape.constrained_delaunay_triangles_in_domain(refinement, domain))?;
        self.parts_to_array(publish_triangle_shapes_for_domain(
            triangles, domain, metric,
        )?)
    }

    pub(crate) fn polygon_triangles_impl(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        let shape = Arc::clone(&self.shape);
        if shape.earcut_packed_axes().is_some() {
            // The common receipt is a geographic sink input; Cartesian rows
            // keep the direct emitter.
            if !matches!(
                geometry::topology_domain(&self.frame),
                Ok(geometry::TopologyDomain::Geographic(_))
            ) {
                let vertices = py.detach(move || shape.polygon_triangle_vertices())?;
                return if vertices.is_empty() {
                    Ok(PyGeometryArray::from_shapes(
                        Vec::new(),
                        self.frame.clone(),
                    )?)
                } else {
                    PyGeometryArray::packed_triangles(&vertices, self.frame.clone())
                };
            }
            let publication = py.detach(move || {
                let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
                shape.earcut_mesh_publication_budgeted(&mut budget)
            })?;
            let vertices = publication.points.as_slice();
            return if vertices.is_empty() {
                Ok(PyGeometryArray::from_shapes(
                    Vec::new(),
                    self.frame.clone(),
                )?)
            } else {
                PyGeometryArray::packed_triangles(vertices, self.frame.clone())
            };
        }
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let topology = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch);
        let (domain, metric) = match topology {
            Ok(topology) => topology,
            Err(error) => {
                py.detach(move || shape.polygon_triangles())?;
                return Err(error);
            },
        };
        let triangles = match domain {
            geometry::TopologyDomain::Cartesian => py.detach(move || shape.polygon_triangles())?,
            geometry::TopologyDomain::Geographic(_) => {
                let publication = py.detach(move || {
                    let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
                    shape.earcut_mesh_publication_budgeted(&mut budget)
                })?;
                Shape::earcut_triangle_shapes_from_publication(&publication)?
            },
        };
        self.parts_to_array(publish_triangle_shapes_for_domain(
            triangles, domain, metric,
        )?)
    }

    pub(crate) fn sample_points_impl(
        &self,
        py: Python<'_>,
        count: &Bound<'_, PyAny>,
        seed: &Bound<'_, PyAny>,
    ) -> PyResult<PyGeometryArray> {
        let count = parse_sample_count(count)?;
        let seed = parse_sample_seed(seed)?;
        let domain = if count == 0 {
            crate::geometry::TopologyDomain::Cartesian
        } else {
            crate::geometry::topology_domain(&self.frame)?
        };
        // A scalar IS row 0: derive its stream exactly as the array lane
        // derives row 0's, so `arr.sample_points(n, seed=s)[0]` and
        // `arr[0].sample_points(n, seed=s)` agree. Using the raw seed here made
        // the two spellings of one operation disagree for the same input.
        let shape = Arc::clone(&self.shape);
        let points = py.detach(move || {
            shape
                .sample_point_receipts_in_domain(
                    count,
                    crate::geometry::row_sample_seed(seed, 0),
                    0,
                    domain,
                )
                .map(crate::geometry::PreparedSampling::into_points)
        })?;
        self.parts_to_array(points.into_iter().map(Shape::Point))
    }

    pub(crate) fn voronoi_cells_impl(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<PyGeometryArray> {
        voronoi_flatten(
            py,
            std::iter::once(Arc::clone(&self.shape)),
            self.frame.clone(),
            tolerance,
            clip,
            "voronoi_cells",
            Shape::voronoi_polygon_outputs_in_frame_budgeted,
        )
    }

    pub(crate) fn voronoi_edge_lines_impl(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<PyGeometryArray> {
        voronoi_edge_lines_flatten(
            py,
            std::iter::once(Arc::clone(&self.shape)),
            self.frame.clone(),
            tolerance,
            clip,
        )
    }

    pub(crate) fn voronoi_impl(
        &self,
        py: Python<'_>,
        output: VoronoiOutput,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<PyGeometryArray> {
        match output {
            VoronoiOutput::Polygons => self.voronoi_cells_impl(py, tolerance, clip),
            VoronoiOutput::Edges => self.voronoi_edge_lines_impl(py, tolerance, clip),
        }
    }

    pub(crate) fn split_shape_detached(
        py: Python<'_>,
        shape: Arc<crate::geometry::ShapeData>,
        splitters: Vec<crate::geometry::Point>,
        frame: Frame,
        tolerance: f64,
    ) -> PyResult<PyGeometryArray> {
        let shapes = py.detach(move || shape.split_at_points(&splitters, tolerance))?;
        Ok(PyGeometryArray::from_shapes(shapes, frame)?)
    }

    pub(crate) fn polygonize_impl(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        let shape = Arc::clone(&self.shape);
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        let polygons = py.detach(move || {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic polygonize source ordinates require the operation's CRS metric",
                    )
                })
            };
            let context = crate::geometry::SourceArcContext::new(domain, Some(&metric_access));
            shape.polygonize_with_context(false, &context)
        })?;
        self.parts_to_array(polygons)
    }

    pub(crate) fn line_merge_impl(&self, py: Python<'_>) -> PyResult<Typed> {
        let shape = Arc::clone(&self.shape);
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        let merged = py.detach(move || {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic line_merge source ordinates require the operation's CRS metric",
                    )
                })
            };
            let context = crate::geometry::SourceArcContext::new(domain, Some(&metric_access));
            shape.line_merge_with_context(&context)
        })?;
        Ok(self.typed_shape(merged))
    }

    /// `tolerance` is already validated non-negative at the free-function boundary.
    pub(crate) fn split(
        &self,
        py: Python<'_>,
        splitter: &Self,
        tolerance: f64,
    ) -> PyResult<PyGeometryArray> {
        Frame::compatible_parts(
            self.crs_ref(),
            self.epoch(),
            splitter.crs_ref(),
            splitter.epoch(),
            "split",
        )?;
        Self::split_shape_detached(
            py,
            Arc::clone(&self.shape),
            splitter.shape.shape().splitter_points()?,
            self.frame.clone(),
            tolerance,
        )
    }

    pub(crate) fn extremes_impl(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        // An extent accessor describes what is THERE: `None` on an empty
        // scalar, exactly like `bounds` and `min_z`, matching the array form's
        // per-row degradation.
        let Some(points) = self.shape.extremes() else {
            return Ok(None);
        };
        let make = |point| Typed(Self::with_frame(Shape::Point(point), self.frame.clone()));
        let typed = [
            make(points[0]),
            make(points[1]),
            make(points[2]),
            make(points[3]),
        ];
        let result = support::extreme_points_type(py)?.call1(PyTuple::new(py, typed)?)?;
        Ok(Some(result.unbind()))
    }

    pub(crate) fn subdivide_parts(
        &self,
        py: Python<'_>,
        max_vertices: i64,
    ) -> PyResult<PyGeometryArray> {
        let max_vertices = validate_subdivide_max_vertices(max_vertices)?;
        let shape = Arc::clone(&self.shape);
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        let parts = py.detach(move || {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic subdivision source ordinates require the operation's CRS metric",
                    )
                })
            };
            shape.subdivide_with_context(
                max_vertices,
                false,
                &crate::geometry::SourceArcContext::new(domain, Some(&metric_access)),
            )
        })?;
        self.parts_to_array(parts)
    }
}
