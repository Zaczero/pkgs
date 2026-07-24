#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Internal geometry operation helpers.
//!
//! Free `#[pyfunction]` surfaces call these instead of exposing thin method
//! delegates on `Geometry`.

use pyo3::types::{PyBytes, PyTuple};

use super::*;

impl PyGeometry {
    pub(crate) fn parts_to_array(
        &self,
        shapes: impl IntoIterator<Item = Shape>,
    ) -> PyGeometryArray {
        PyGeometryArray::pack_or_mixed(
            shapes
                .into_iter()
                .map(|shape| self.with_shape(shape))
                .collect(),
            self.frame.clone(),
        )
    }

    pub(crate) fn to_wkt_impl(
        &self,
        output_dimension: Option<&Bound<'_, PyAny>>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<String> {
        let output_dimension = parse_wkt_output_dimension(output_dimension)?;
        let quantized = precision
            .map(parse_precision)
            .transpose()?
            .map(|precision| self.shape.quantize(precision));
        Ok(io::to_wkt_with_dimension(
            quantized.as_ref().unwrap_or(&self.shape),
            output_dimension,
            self.crs_str(),
            include_srid,
        )?)
    }

    pub(crate) fn to_wkb_impl<'py>(
        &self,
        py: Python<'py>,
        include_srid: bool,
        precision: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let quantized = precision
            .map(parse_precision)
            .transpose()?
            .map(|precision| self.shape.quantize(precision));
        Ok(PyBytes::new(
            py,
            &io::to_wkb(
                quantized.as_ref().unwrap_or(&self.shape),
                self.crs_str(),
                include_srid,
            )?,
        ))
    }

    pub(crate) fn to_geojson_impl(&self, include_z: bool) -> PyResult<String> {
        require_geojson_crs(self.crs_str())?;
        if self.shape.has_m() {
            return Err(InvalidGeometryError::new_err(
                "GeoJSON has no M ordinate; remove M with set_m(None), or use WKT/GeoArrow",
            ));
        }
        // A WGS84-tagged frame opts into the RFC 7946 geographic rules (domain
        // validation, antimeridian cutting); CRS-free geometry is planar.
        let geographic = self.crs_str().is_some();
        Ok(if include_z {
            io::to_geojson_string::<true>(&self.shape, geographic)?
        } else {
            io::to_geojson_string::<false>(&self.shape, geographic)?
        })
    }

    pub(crate) fn validate_impl(&self) -> PyValidationReport {
        PyValidationReport {
            geometry: self.clone(),
            issue: geometry::validate_data_in_frame(
                &self.shape,
                geometry::is_geographic_frame(&self.frame),
            ),
        }
    }

    pub(crate) fn repair_impl(&self, py: Python<'_>, method: RepairMethod) -> PyResult<Typed> {
        let geographic = geometry::is_geographic_frame(&self.frame);
        let repaired =
            py.detach(|| geometry::repair_data_in_frame(&self.shape, geographic, method))?;
        Ok(Typed(repaired.map_or_else(
            || self.clone(),
            |shape| self.with_shape(shape),
        )))
    }

    pub(crate) fn self_intersections_impl(&self) -> PyGeometryArray {
        let points = geometry::self_intersections_in_frame(
            self.shape.shape(),
            geometry::is_geographic_frame(&self.frame),
        );
        self.parts_to_array(points.into_iter().map(Shape::Point))
    }

    pub(crate) fn delaunay_triangles_impl(&self) -> PyResult<PyGeometryArray> {
        // Build the packed `Polygons` layout straight from the triangulation's
        // flat vertex stream — one coords column + arithmetic CSR offsets — so a
        // 1000-triangle result skips 1000 per-triangle `Polygon`/`CoordSeq`
        // allocations and the re-pack scan (measured ~50% of the op).
        let vertices = self.shape.delaunay_triangle_vertices();
        if vertices.is_empty() {
            return Ok(PyGeometryArray::pack_or_mixed(
                Vec::new(),
                self.frame.clone(),
            ));
        }
        let axes = vertices.first().map(|point| point.axes);
        if vertices.iter().any(|point| Some(point.axes) != axes) {
            return Ok(self.parts_to_array(self.shape.delaunay_triangles()?));
        }
        PyGeometryArray::packed_triangles(&vertices, self.frame.clone())
    }

    pub(crate) fn constrained_delaunay_triangles_impl(
        &self,
        min_angle: Option<&Bound<'_, PyAny>>,
        max_area: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyGeometryArray> {
        let refinement = parse_cdt_refinement(min_angle, max_area)?;
        // Refinement inserts Steiner vertices, so output is XY-only. The 2D
        // fast path also applies to plain XY inputs; Z/M inputs keep the
        // per-shape path whose `carry_each` resolves corners when not refining.
        if refinement.active() || (!self.shape.has_z() && !self.shape.has_m()) {
            let vertices = self.shape.constrained_delaunay_vertices(refinement)?;
            return if vertices.is_empty() {
                Ok(PyGeometryArray::pack_or_mixed(
                    Vec::new(),
                    self.frame.clone(),
                ))
            } else {
                PyGeometryArray::packed_triangles(&vertices, self.frame.clone())
            };
        }
        Ok(self.parts_to_array(self.shape.constrained_delaunay_triangles(refinement)?))
    }

    pub(crate) fn polygon_triangles_impl(&self) -> PyResult<PyGeometryArray> {
        Ok(self.parts_to_array(self.shape.polygon_triangles()?))
    }

    pub(crate) fn sample_points_impl(
        &self,
        count: &Bound<'_, PyAny>,
        seed: &Bound<'_, PyAny>,
    ) -> PyResult<PyGeometryArray> {
        let count = parse_sample_count(count)?;
        let seed = parse_sample_seed(seed)?;
        let points = self.shape.sample_points(count, seed)?;
        Ok(self.parts_to_array(points.into_iter().map(Shape::Point)))
    }

    pub(crate) fn voronoi_polygons_impl(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<PyGeometryArray> {
        voronoi_flatten(
            py,
            std::iter::once(self.shape.shape()),
            self.frame.clone(),
            tolerance,
            clip,
            "voronoi_polygons",
            Shape::voronoi_polygons,
        )
    }

    pub(crate) fn voronoi_edges_impl(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<PyGeometryArray> {
        voronoi_flatten(
            py,
            std::iter::once(self.shape.shape()),
            self.frame.clone(),
            tolerance,
            clip,
            "voronoi_edges",
            Shape::voronoi_edges,
        )
    }

    pub(crate) fn polygonize_impl(&self) -> PyResult<PyGeometryArray> {
        Ok(self.parts_to_array(self.shape.polygonize(false)?))
    }

    pub(crate) fn line_merge_impl(&self) -> PyResult<Typed> {
        Ok(self.typed_shape(self.shape.line_merge()?))
    }

    /// `tolerance` is already validated non-negative at the free-function boundary.
    pub(crate) fn split(&self, splitter: &Self, tolerance: f64) -> PyResult<PyGeometryArray> {
        Frame::compatible_parts(
            self.crs_ref(),
            self.epoch(),
            splitter.crs_ref(),
            splitter.epoch(),
            "split",
        )?;
        Ok(self.parts_to_array(self.shape.split(&splitter.shape, tolerance)?))
    }

    pub(crate) fn extremes_impl(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let Some(points) = self.shape.extremes() else {
            return Err(crate::py::errors::InvalidGeometryError::new_err(
                "extremes requires a non-empty geometry",
            ));
        };
        let typed = points.map(|point| {
            Typed(Self::with_epoch(
                Shape::Point(point),
                self.crs_ref().cloned(),
                self.epoch(),
            ))
        });
        let result =
            crate::py::support::extreme_points_type(py)?.call1(PyTuple::new(py, typed)?)?;
        Ok(result.unbind())
    }

    pub(crate) fn spatial_key_impl(
        &self,
        curve: crate::py::support::SpatialCurve,
        level: i64,
        bounds: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        crate::py::support::spatial_key_for_shape(&self.shape, curve.into(), level, bounds)
    }

    pub(crate) fn subdivide_parts(&self, max_vertices: i64) -> PyResult<PyGeometryArray> {
        let max_vertices = validate_subdivide_max_vertices(max_vertices)?;
        Ok(self.parts_to_array(self.shape.subdivide(max_vertices, false)?))
    }
}
