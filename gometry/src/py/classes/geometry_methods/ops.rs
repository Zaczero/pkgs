//! Internal geometry operation helpers.
//!
//! Free `#[pyfunction]` surfaces call these instead of exposing thin method
//! delegates on `Geometry`.

use std::sync::Arc;

use pyo3::types::{PyBytes, PyTuple};

use crate::geometry::publication::{PublicationRows, publish_split_parts_array};
use crate::geometry::storage::replacement::{CoordinateReplacement, replace_shape_coordinates};
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

fn publish_earcut_publication_for_domain(
    publication: geometry::PreparedMeshPublication,
    domain: geometry::TopologyDomain,
    metric: crate::dispatch::MetricCtx<'_>,
    budget: &mut geometry::ExpansionBudget,
) -> crate::error::Result<geometry::PublishedTriangleVertices> {
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

impl PyGeometry {
    pub(crate) fn replace_coordinates_impl(
        &self,
        py: Python<'_>,
        replacement: CoordinateReplacement,
    ) -> PyResult<Typed> {
        let shape = if matches!(self.shape.shape(), Shape::Point(_) | Shape::Empty(..)) {
            replace_shape_coordinates(self.shape.shape(), &replacement)?
        } else {
            py.detach(move || replace_shape_coordinates(self.shape.shape(), &replacement))?
        };
        Ok(self.typed_shape(shape))
    }

    pub(crate) fn parts_to_array(
        &self,
        shapes: impl IntoIterator<Item = Shape>,
    ) -> crate::error::Result<PyGeometryArray> {
        // Shape-native sink: never stage per-part `PyGeometry` wrappers.
        PyGeometryArray::from_shapes(shapes.into_iter().collect(), self.frame.clone())
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
        Ok(py.detach(move || {
            let points = geometry::self_intersections_data_in_frame(&plan, domain)?;
            self.parts_to_array(points.into_iter().map(Shape::Point))
        })?)
    }

    pub(crate) fn delaunay_triangles_impl(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        let shape = Arc::clone(&self.shape);
        let frame = self.frame.clone();
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        Ok(py.detach(move || {
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
            let published = geometry::publish_mesh_publication_for_domain(
                publication,
                domain,
                &source_context,
                &mut budget,
            )?;
            if domain.is_geographic() {
                let finished = geometry::finish_mesh_publication_array(published, frame)?;
                PyGeometryArray::from_finished(finished)
            } else {
                PyGeometryArray::packed_triangles_with_ring_ends(
                    &published.points,
                    &published.ring_ends,
                    frame,
                )
            }
        })?)
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
        if matches!(domain, geometry::TopologyDomain::Geographic(_)) {
            let shape = Arc::clone(&self.shape);
            return Ok(py.detach(move || {
                let mut budget =
                    geometry::ExpansionBudget::new("triangulate", "min_angle/max_area");
                let drawing = geometry::prepare_geographic_cdt_drawing(
                    &shape,
                    refinement,
                    domain,
                    metric,
                    &mut budget,
                )?;
                geometry::publish_cdt_array(drawing, self.frame.clone())
            })?);
        }
        // Refinement inserts Steiner vertices, so output is XY-only. The
        // source-site mesh plan supplies the same selected points and axes to
        // both the packed and per-shape publication paths.
        Ok(py.detach(|| {
            if refinement.active() || (!self.shape.has_z() && !self.shape.has_m()) {
                let shape = Arc::clone(&self.shape);
                let vertices = shape.constrained_delaunay_vertices_in_domain(refinement, domain)?;
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
            let triangles = shape.constrained_delaunay_triangles_in_domain(refinement, domain)?;
            self.parts_to_array(triangles)
        })?)
    }

    pub(crate) fn polygon_triangles_impl(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        let shape = Arc::clone(&self.shape);
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let topology = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch);
        let (domain, metric) = match topology {
            Ok(topology) => topology,
            Err(error) => {
                py.detach(move || {
                    if shape.earcut_packed_axes().is_some() {
                        shape.polygon_triangle_vertices().map(|_| ())
                    } else {
                        shape.polygon_triangles().map(|_| ())
                    }
                })?;
                return Err(error.into());
            },
        };
        Ok(py.detach(move || match domain {
            geometry::TopologyDomain::Cartesian => {
                if shape.earcut_packed_axes().is_some() {
                    let vertices = shape.polygon_triangle_vertices()?;
                    PyGeometryArray::packed_triangles(&vertices, self.frame.clone())
                } else {
                    let triangles = shape.polygon_triangles()?;
                    self.parts_to_array(triangles)
                }
            },
            geometry::TopologyDomain::Geographic(_) => {
                let frame = self.frame.clone();
                {
                    let mut budget = crate::geometry::ExpansionBudget::new("triangulate", "method");
                    let publication = shape.earcut_mesh_publication_budgeted(&mut budget)?;
                    let published = publish_earcut_publication_for_domain(
                        publication,
                        domain,
                        metric,
                        &mut budget,
                    )?;
                    let finished = geometry::finish_mesh_publication_array(published, frame)?;
                    PyGeometryArray::from_finished(finished)
                }
            },
        })?)
    }

    pub(crate) fn sample_points_impl(
        &self,
        py: Python<'_>,
        count: &Bound<'_, PyAny>,
        seed: Option<&Bound<'_, PyAny>>,
        unit: Option<crate::DistanceUnit>,
    ) -> PyResult<PyGeometryArray> {
        let count = parse_sample_count(count)?;
        let seed = crate::boundary::input::resolve_sample_seed(py, seed)?;
        let seed = parse_sample_seed(&seed)?;
        let model = crate::broadcast::resolve_metric(
            self.frame.crs_str(),
            unit,
            crate::dispatch::MetricOp::SamplePoints,
        )?;
        let metric = crate::crs::ResolvedMetric::from_model(&model)?;
        let metric = match &metric {
            crate::crs::ResolvedMetric::Geodesic(metric) => Some(metric),
            crate::crs::ResolvedMetric::Planar { .. } => None,
        };
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
        Ok(py.detach(move || {
            let prepared = shape.sample_point_receipts_in_metric(
                count,
                crate::geometry::row_sample_seed(seed, 0),
                0,
                domain,
                metric,
            )?;
            if domain.is_geographic() {
                geometry::publish_sampling_array(prepared, self.frame.clone())
            } else {
                let points = prepared.into_points();
                self.parts_to_array(points.into_iter().map(Shape::Point))
            }
        })?)
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
        let axes = shape.axes();
        let mut metric_scratch = None;
        let source_context = if axes.has_z() || axes.has_m() {
            let scratch =
                metric_scratch.get_or_insert_with(crate::dispatch::MetricScratch::default);
            let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
                .resolve_topology_ctx(&frame, scratch)?;
            crate::geometry::source_arc_ordinates_eligible(axes, domain).then_some((domain, metric))
        } else {
            None
        };
        Ok(py.detach(move || {
            let shapes = match source_context {
                Some((domain, metric)) => {
                    let metric_access = || {
                        metric.geographic_chart_metric()?.ok_or_else(|| {
                        crate::crs::CrsError::invalid(
                            "geographic split source ordinates require the operation's CRS metric",
                        )
                    })
                    };
                    let context =
                        crate::geometry::SourceArcContext::new(domain, Some(&metric_access));
                    shape.split_at_points_with_source_arcs(&splitters, tolerance, &context)
                }?,
                None => shape.split_at_points(&splitters, tolerance)?,
            };
            publish_split_parts_array(shapes, frame)
        })?)
    }

    pub(crate) fn polygonize_impl(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        let shape = Arc::clone(&self.shape);
        let output_frame = self.frame.clone();
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = crate::dispatch::MetricResolver::GeographicCarry
            .resolve_topology_ctx(&self.frame, &mut metric_scratch)?;
        if domain.is_geographic() {
            let array = py.detach(move || {
                let metric_access = || {
                    metric.geographic_chart_metric()?.ok_or_else(|| {
                        crate::crs::CrsError::invalid(
                            "geographic polygonize source ordinates require the operation's CRS metric",
                        )
                    })
                };
                let context = crate::geometry::SourceArcContext::new(domain, Some(&metric_access));
                let mut rows = PublicationRows::with_producer_drawing_capacity(0);
                crate::geometry::Shape::append_polygonize_rows(
                    &[shape.shape()],
                    false,
                    &context,
                    &output_frame,
                    None,
                    &mut rows,
                )?;
                rows.finish(output_frame)
            })?;
            drop(metric_scratch);
            return Ok(array);
        }
        Ok(py.detach(move || {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic polygonize source ordinates require the operation's CRS metric",
                    )
                })
            };
            let context = crate::geometry::SourceArcContext::new(domain, Some(&metric_access));
            let polygons =
                shape.polygonize_with_publication_frame(false, &context, output_frame)?;
            self.parts_to_array(polygons)
        })?)
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
        let points = if matches!(self.shape.shape(), Shape::Point(_) | Shape::Empty(..)) {
            self.shape.extremes()
        } else {
            py.detach(|| self.shape.extremes())
        };
        let Some(points) = points else {
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
        Ok(py.detach(move || {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic subdivision source ordinates require the operation's CRS metric",
                    )
                })
            };
            let parts = shape.subdivide_with_context(
                max_vertices,
                false,
                &crate::geometry::SourceArcContext::new(domain, Some(&metric_access)),
            )?;
            self.parts_to_array(parts)
        })?)
    }
}

#[cfg(test)]
mod coordinate_replacement_tests {
    use pyo3::prelude::*;
    use pyo3::types::{PyCFunction, PyDict};

    use super::*;
    use crate::test_support::{boundary_probe, initialize_python};

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn scalar_application_preserves_axes_and_only_detaches_shape_rebuilds() {
        initialize_python();
        Python::attach(|py| {
            for (wkt, xs, expected, attached) in [
                ("POINT ZM (1 2 3 4)", vec![10.], "POINT ZM (10 2 3 4)", true),
                ("POINT Z EMPTY", vec![], "POINT Z EMPTY", true),
                (
                    "LINESTRING ZM (0 0 3 30, 1 1 4 40)",
                    vec![10., 11.],
                    "LINESTRING ZM (10 0 3 30, 11 1 4 40)",
                    false,
                ),
                (
                    "GEOMETRYCOLLECTION (POINT ZM (1 2 3 4), LINESTRING (0 0, 1 1))",
                    vec![10., 11., 12.],
                    "GEOMETRYCOLLECTION (POINT ZM (10 2 3 4), LINESTRING (11 0, 12 1))",
                    false,
                ),
            ] {
                let geometry =
                    PyGeometry::with_frame(crate::io::parse_wkt(wkt).unwrap(), Frame::None);
                let kwargs = PyDict::new(py);
                kwargs.set_item("x", xs).unwrap();
                let (result, events) = boundary_probe::capture(|| {
                    geometry.with_coordinates(py, &PyTuple::empty(py), Some(&kwargs))
                });
                assert_eq!(
                    result.unwrap().0.shape.shape(),
                    &crate::io::parse_wkt(expected).unwrap()
                );
                assert_eq!(events, [
                    ("shape replacement", attached),
                    ("shape replacement finished", attached),
                ]);
            }
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn scalar_callback_precedes_detached_replacement() {
        initialize_python();
        Python::attach(|py| {
            let geometry = Py::new(
                py,
                PyGeometry::with_frame(
                    crate::io::parse_wkt("LINESTRING (0 0, 1 1)").unwrap(),
                    Frame::None,
                ),
            )
            .unwrap()
            .into_bound(py);
            let callback = PyCFunction::new_closure(py, None, None, |args, _kwargs| {
                boundary_probe::record("callback");
                args.get_item(0)?.call_method0("copy").map(Bound::unbind)
            })
            .unwrap();
            let (result, events) = boundary_probe::capture(|| {
                PyGeometry::map_coordinates(&geometry, py, callback.as_any())
            });
            assert_eq!(
                result.unwrap().0.shape.shape(),
                &crate::io::parse_wkt("LINESTRING (0 0, 1 1)").unwrap()
            );
            assert_eq!(events, [
                ("callback", true),
                ("shape replacement", false),
                ("shape replacement finished", false),
            ]);
        });
    }
}
