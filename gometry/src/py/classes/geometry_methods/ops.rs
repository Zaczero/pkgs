#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Internal geometry operation helpers.
//!
//! Free `#[pyfunction]` surfaces call these instead of exposing thin method
//! delegates on `Geometry`.

use std::sync::Arc;

use pyo3::PyErr;
use pyo3::types::{PyBytes, PyTuple};

use crate::py::classes::geometry_methods::{
    Bound, InvalidGeometryError, Py, PyAny, PyAnyMethods as _, PyGeometry, PyResult, Python, Typed,
};
use crate::{
    Frame, PyGeometryArray, PyValidationReport, RepairMethod, Shape, VoronoiClipInput, geometry,
    io, parse_cdt_refinement, parse_precision, parse_sample_count, parse_sample_seed,
    parse_wkt_output_dimension, require_geojson_crs, validate_subdivide_max_vertices,
    voronoi_edges_flatten, voronoi_flatten,
};

impl PyGeometry {
    pub(crate) fn parts_to_array(
        &self,
        shapes: impl IntoIterator<Item = Shape>,
    ) -> PyResult<PyGeometryArray> {
        // Shape-native sink: never stage per-part `PyGeometry` wrappers.
        Ok(PyGeometryArray::from_shapes(
            crate::try_collect_results(shapes.into_iter().map(Ok::<_, PyErr>))?,
            self.frame.clone(),
        ))
    }

    pub(crate) fn to_wkt_impl(
        &self,
        py: Python<'_>,
        output_dimension: Option<&Bound<'_, PyAny>>,
        include_srid: bool,
        precision: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<String> {
        let output_dimension = parse_wkt_output_dimension(output_dimension)?;
        let precision = precision.map(parse_precision).transpose()?;
        let shape = Arc::clone(&self.shape);
        let crs = self.crs_ref().cloned();
        py.detach(move || {
            let quantized = precision
                .map(|precision| shape.quantize(precision))
                .transpose()?;
            Ok(io::to_wkt_with_dimension(
                quantized.as_ref().unwrap_or(&shape),
                output_dimension,
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

    pub(crate) fn to_geojson_impl(&self, py: Python<'_>, include_z: bool) -> PyResult<String> {
        require_geojson_crs(self.crs_str())?;
        if self.shape.has_m() {
            return Err(InvalidGeometryError::new_err(
                "GeoJSON has no M ordinate; remove M with set_m(None), or use WKT/GeoArrow",
            ));
        }
        // A WGS84-tagged frame opts into the RFC 7946 geographic rules (domain
        // validation, antimeridian cutting); CRS-free geometry is planar.
        let shape = Arc::clone(&self.shape);
        let frame = self.frame.clone();
        py.detach(move || {
            let domain = geometry::topology_domain(&frame)?;
            Ok(if include_z {
                io::to_geojson_string::<true>(&shape, domain)?
            } else {
                io::to_geojson_string::<false>(&shape, domain)?
            })
        })
    }

    pub(crate) fn validate_impl(&self, py: Python<'_>) -> PyValidationReport {
        let geometry = self.clone();
        let plan = self.plan_or_transient();
        let domain = geometry::topology_domain(&self.frame)
            .unwrap_or(crate::geometry::TopologyDomain::Cartesian);
        PyValidationReport {
            geometry,
            issue: py.detach(move || geometry::validate_data_in_frame(&plan, domain)),
        }
    }

    pub(crate) fn repair_impl(&self, py: Python<'_>, method: RepairMethod) -> PyResult<Typed> {
        let domain = geometry::topology_domain(&self.frame)?;
        let repaired = py
            .detach(|| geometry::repair_data_in_frame(&self.plan_or_transient(), domain, method))?;
        Ok(Typed(repaired.map_or_else(
            || self.clone(),
            |shape| self.with_shape(shape),
        )))
    }

    pub(crate) fn self_intersections_impl(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        let shape = Arc::clone(&self.shape);
        let domain = geometry::topology_domain(&self.frame)?;
        let points =
            py.detach(move || geometry::self_intersections_in_frame(shape.shape(), domain));
        self.parts_to_array(points.into_iter().map(Shape::Point))
    }

    pub(crate) fn delaunay_triangles_impl(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        // Build the packed `Polygons` layout straight from the triangulation's
        // flat vertex stream — one coords column + arithmetic CSR offsets — so a
        // 1000-triangle result skips 1000 per-triangle `Polygon`/`CoordSeq`
        // allocations and the re-pack scan (measured ~50% of the op).
        let shape = Arc::clone(&self.shape);
        let vertices = py.detach(move || shape.delaunay_triangle_vertices())?;
        if vertices.is_empty() {
            return Ok(PyGeometryArray::from_shapes(Vec::new(), self.frame.clone()));
        }
        let axes = vertices.first().map(|point| point.axes);
        if vertices.iter().any(|point| Some(point.axes) != axes) {
            let shape = Arc::clone(&self.shape);
            let triangles = py.detach(move || shape.delaunay_triangles())?;
            return self.parts_to_array(triangles);
        }
        PyGeometryArray::packed_triangles(&vertices, self.frame.clone())
    }

    pub(crate) fn constrained_delaunay_triangles_impl(
        &self,
        py: Python<'_>,
        min_angle: Option<&Bound<'_, PyAny>>,
        max_area: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyGeometryArray> {
        let refinement = parse_cdt_refinement(min_angle, max_area)?;
        // Refinement inserts Steiner vertices, so output is XY-only. The 2D
        // fast path also applies to plain XY inputs; Z/M inputs keep the
        // per-shape path whose `carry_each` resolves corners when not refining.
        if refinement.active() || (!self.shape.has_z() && !self.shape.has_m()) {
            let shape = Arc::clone(&self.shape);
            let vertices = py.detach(move || shape.constrained_delaunay_vertices(refinement))?;
            return if vertices.is_empty() {
                Ok(PyGeometryArray::from_shapes(Vec::new(), self.frame.clone()))
            } else {
                PyGeometryArray::packed_triangles(&vertices, self.frame.clone())
            };
        }
        let shape = Arc::clone(&self.shape);
        let triangles = py.detach(move || shape.constrained_delaunay_triangles(refinement))?;
        self.parts_to_array(triangles)
    }

    pub(crate) fn polygon_triangles_impl(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        let shape = Arc::clone(&self.shape);
        if shape.earcut_packed_axes().is_some() {
            let vertices = py.detach(move || shape.polygon_triangle_vertices())?;
            return if vertices.is_empty() {
                Ok(PyGeometryArray::from_shapes(Vec::new(), self.frame.clone()))
            } else {
                PyGeometryArray::packed_triangles(&vertices, self.frame.clone())
            };
        }
        self.parts_to_array(py.detach(move || shape.polygon_triangles())?)
    }

    pub(crate) fn sample_points_impl(
        &self,
        py: Python<'_>,
        count: &Bound<'_, PyAny>,
        seed: &Bound<'_, PyAny>,
    ) -> PyResult<PyGeometryArray> {
        let count = parse_sample_count(count)?;
        let seed = parse_sample_seed(seed)?;
        // A scalar IS row 0: derive its stream exactly as the array lane
        // derives row 0's, so `arr.sample_points(n, seed=s)[0]` and
        // `arr[0].sample_points(n, seed=s)` agree. Using the raw seed here made
        // the two spellings of one operation disagree for the same input.
        let shape = Arc::clone(&self.shape);
        let points = py.detach(move || {
            shape.sample_points(count, crate::geometry::row_sample_seed(seed, 0))
        })?;
        self.parts_to_array(points.into_iter().map(Shape::Point))
    }

    pub(crate) fn voronoi_polygons_impl(
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
            "voronoi_polygons",
            Shape::voronoi_polygons_budgeted,
        )
    }

    pub(crate) fn voronoi_edges_impl(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: &VoronoiClipInput,
    ) -> PyResult<PyGeometryArray> {
        voronoi_edges_flatten(
            py,
            std::iter::once(Arc::clone(&self.shape)),
            self.frame.clone(),
            tolerance,
            clip,
        )
    }

    pub(crate) fn split_shape_detached(
        py: Python<'_>,
        shape: Arc<crate::geometry::ShapeData>,
        splitter: Shape,
        frame: Frame,
        tolerance: f64,
    ) -> PyResult<PyGeometryArray> {
        let shapes = py.detach(move || shape.split(&splitter, tolerance))?;
        Ok(PyGeometryArray::from_shapes(shapes, frame))
    }

    pub(crate) fn polygonize_impl(&self, py: Python<'_>) -> PyResult<PyGeometryArray> {
        let shape = Arc::clone(&self.shape);
        self.parts_to_array(py.detach(move || shape.polygonize(false))?)
    }

    pub(crate) fn line_merge_impl(&self, py: Python<'_>) -> PyResult<Typed> {
        let shape = Arc::clone(&self.shape);
        Ok(self.typed_shape(py.detach(move || shape.line_merge())?))
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
            splitter.shape.shape().clone(),
            self.frame.clone(),
            tolerance,
        )
    }

    pub(crate) fn extremes_impl(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        // An extent accessor describes what is THERE: `None` on an empty
        // scalar, exactly like `bounds`, `bounds_3d`, `min_z` and `z_range`.
        // Raising made these two the only members of that family that could
        // not be called on an empty geometry, while both ARRAY forms already
        // degraded per row.
        let Some(points) = self.shape.extremes() else {
            return Ok(None);
        };
        let typed =
            points.map(|point| Typed(Self::with_frame(Shape::Point(point), self.frame.clone())));
        let result =
            crate::py::support::extreme_points_type(py)?.call1(PyTuple::new(py, typed)?)?;
        Ok(Some(result.unbind()))
    }

    pub(crate) fn spatial_key_impl(
        &self,
        curve: crate::py::support::SpatialCurve,
        level: i64,
        bounds: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<u64>> {
        // `None` on an empty scalar — see `extremes_impl`.
        crate::py::support::spatial_key_for_shape_opt(&self.shape, curve.into(), level, bounds)
    }

    pub(crate) fn subdivide_parts(
        &self,
        py: Python<'_>,
        max_vertices: i64,
    ) -> PyResult<PyGeometryArray> {
        let max_vertices = validate_subdivide_max_vertices(max_vertices)?;
        let shape = Arc::clone(&self.shape);
        self.parts_to_array(py.detach(move || shape.subdivide(max_vertices, false))?)
    }
}
