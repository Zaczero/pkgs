#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Internal geometry operation helpers.
//!
//! Free `#[pyfunction]` surfaces call these instead of exposing thin method
//! delegates on `Geometry`.

use pyo3::types::{PyBytes, PyTuple};

use crate::py::classes::geometry_methods::{
    Bound, InvalidGeometryError, Py, PyAny, PyAnyMethods as _, PyGeometry, PyResult, Python, Typed,
};
use crate::{
    Frame, PyGeometryArray, PyValidationReport, RepairMethod, Shape, VoronoiClipInput, geometry,
    io, parse_cdt_refinement, parse_precision, parse_sample_count, parse_sample_seed,
    parse_wkt_output_dimension, require_geojson_crs, validate_subdivide_max_vertices,
    voronoi_flatten,
};

/// Byte-size gate for exact `PyBytes::new_with` (not a coordinate-count cliff).
const EXACT_PYBYTES_MIN_BYTES: usize = 4096;

/// Cheap overestimate of WKB payload bytes without classifying the shape.
/// Used only to skip the exact-into-PyBytes path on ordinary small geometry.
fn wkb_byte_estimate(shape: &crate::geometry::Shape) -> usize {
    use crate::geometry::Shape;
    match shape {
        Shape::Point(_) | Shape::Empty(..) => 32,
        Shape::MultiPoint(points) => points.len().saturating_mul(32).saturating_add(32),
        Shape::LineString(line) => line.len().saturating_mul(16).saturating_add(32),
        Shape::MultiLineString(lines) => lines.iter().fold(32_usize, |acc, line| {
            acc.saturating_add(32)
                .saturating_add(line.len().saturating_mul(16))
        }),
        Shape::Polygon(polygon) => polygon.coord_count().saturating_mul(16).saturating_add(64),
        Shape::MultiPolygon(polygons) => polygons.iter().fold(32_usize, |acc, polygon| {
            acc.saturating_add(64)
                .saturating_add(polygon.coord_count().saturating_mul(16))
        }),
        // Collections always take the growable `io::to_wkb` path.
        Shape::GeometryCollection(_) => 0,
    }
}

impl PyGeometry {
    pub(crate) fn parts_to_array(
        &self,
        shapes: impl IntoIterator<Item = Shape>,
    ) -> PyGeometryArray {
        // Shape-native sink: never stage per-part `PyGeometry` wrappers.
        PyGeometryArray::from_shapes(shapes.into_iter().collect(), self.frame.clone())
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
        let shape = quantized.as_ref().unwrap_or(&self.shape);
        let crs = self.crs_str();
        // No coordinate-count gate (the R7 cliff was `coord_count() < 32`).
        // Byte-size decision via a cheap overestimate (no classify): only large
        // payloads take exact `PyBytes::new_with` (round-4 win). Ordinary
        // multiparts share one classify with `__reduce__` via `io::to_wkb` —
        // never `to_wkb_len` then `to_wkb` (double classify tax on small).
        if wkb_byte_estimate(shape) >= EXACT_PYBYTES_MIN_BYTES
            && let Ok(len) = io::to_wkb_len(shape, crs, include_srid)
            && len >= EXACT_PYBYTES_MIN_BYTES
        {
            return PyBytes::new_with(py, len, |buf| {
                io::write_wkb_into(buf, shape, crs, include_srid)?;
                Ok(())
            });
        }
        let bytes = io::to_wkb(shape, crs, include_srid)?;
        Ok(PyBytes::new(py, &bytes))
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
        let vertices = self.shape.delaunay_triangle_vertices()?;
        if vertices.is_empty() {
            return Ok(PyGeometryArray::from_shapes(Vec::new(), self.frame.clone()));
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
                Ok(PyGeometryArray::from_shapes(Vec::new(), self.frame.clone()))
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
        // A scalar IS row 0: derive its stream exactly as the array lane
        // derives row 0's, so `arr.sample_points(n, seed=s)[0]` and
        // `arr[0].sample_points(n, seed=s)` agree. Using the raw seed here made
        // the two spellings of one operation disagree for the same input.
        let points = self
            .shape
            .sample_points(count, crate::geometry::row_sample_seed(seed, 0))?;
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
            Shape::voronoi_polygons_budgeted,
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
            Shape::voronoi_edges_budgeted,
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

    pub(crate) fn extremes_impl(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        // An extent accessor describes what is THERE: `None` on an empty
        // scalar, exactly like `bounds`, `bounds_3d`, `min_z` and `z_range`.
        // Raising made these two the only members of that family that could
        // not be called on an empty geometry, while both ARRAY forms already
        // degraded per row.
        let Some(points) = self.shape.extremes() else {
            return Ok(None);
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

    pub(crate) fn subdivide_parts(&self, max_vertices: i64) -> PyResult<PyGeometryArray> {
        let max_vertices = validate_subdivide_max_vertices(max_vertices)?;
        Ok(self.parts_to_array(self.shape.subdivide(max_vertices, false)?))
    }
}
