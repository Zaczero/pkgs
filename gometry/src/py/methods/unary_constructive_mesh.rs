#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::py::errors::GeometryError;
use crate::py::support::{TriangulationMethod, VoronoiClipInput};

#[pymethods]
impl PyGeometry {
    #[doc = doc_triangulate!(scalar)]
    #[pyo3(
        signature = (*, method, min_angle = None, max_area = None)
    )]
    pub fn triangulate(
        &self,
        method: TriangulationMethod,
        min_angle: Option<&Bound<'_, PyAny>>,
        max_area: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyGeometryArray> {
        match method {
            TriangulationMethod::Earcut => {
                require_plain_triangulation_options(min_angle, max_area)?;
                self.polygon_triangles_impl()
            },
            TriangulationMethod::Delaunay => {
                require_plain_triangulation_options(min_angle, max_area)?;
                self.delaunay_triangles_impl()
            },
            TriangulationMethod::Constrained => {
                self.constrained_delaunay_triangles_impl(min_angle, max_area)
            },
        }
    }
    #[doc = doc_sample_points!(scalar)]
    #[pyo3(signature = (count, *, seed))]
    pub fn sample_points(
        &self,
        count: &Bound<'_, PyAny>,
        seed: &Bound<'_, PyAny>,
    ) -> PyResult<PyGeometryArray> {
        self.sample_points_impl(count, seed)
    }
    #[doc = doc_voronoi_polygons!(scalar)]
    #[pyo3(
        signature = (*, tolerance = 0.0, clip = VoronoiClipInput::DEFAULT),
        text_signature = "($self, *, tolerance=0.0, clip='padded')"
    )]
    pub fn voronoi_polygons(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: VoronoiClipInput,
    ) -> PyResult<PyGeometryArray> {
        self.voronoi_polygons_impl(py, tolerance, &clip)
    }
    #[doc = doc_voronoi_edges!(scalar)]
    #[pyo3(
        signature = (*, tolerance = 0.0, clip = VoronoiClipInput::DEFAULT),
        text_signature = "($self, *, tolerance=0.0, clip='padded')"
    )]
    pub fn voronoi_edges(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: VoronoiClipInput,
    ) -> PyResult<PyGeometryArray> {
        self.voronoi_edges_impl(py, tolerance, &clip)
    }
}

#[pymethods]
impl PyGeometryArray {
    #[doc = doc_triangulate!(array)]
    #[pyo3(
        signature = (*, method, min_angle = None, max_area = None)
    )]
    pub fn triangulate(
        &self,
        py: Python<'_>,
        method: TriangulationMethod,
        min_angle: Option<&Bound<'_, PyAny>>,
        max_area: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<crate::py::vectors::Groups> {
        match method {
            TriangulationMethod::Earcut => {
                require_plain_triangulation_options(min_angle, max_area)?;
                self.polygon_triangles_impl(py)
            },
            TriangulationMethod::Delaunay => {
                require_plain_triangulation_options(min_angle, max_area)?;
                self.delaunay_triangles_impl(py)
            },
            TriangulationMethod::Constrained => {
                self.constrained_delaunay_triangles_impl(py, min_angle, max_area)
            },
        }
    }
    #[doc = doc_sample_points!(array)]
    #[pyo3(signature = (count, *, seed))]
    pub fn sample_points(
        &self,
        count: &Bound<'_, PyAny>,
        seed: &Bound<'_, PyAny>,
    ) -> PyResult<crate::py::vectors::Groups> {
        self.sample_points_impl(count, seed)
    }
    #[doc = doc_voronoi_polygons!(array)]
    #[pyo3(
        signature = (*, tolerance = 0.0, clip = VoronoiClipInput::DEFAULT),
        text_signature = "($self, *, tolerance=0.0, clip='padded')"
    )]
    pub fn voronoi_polygons(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: VoronoiClipInput,
    ) -> PyResult<crate::py::vectors::Groups> {
        self.voronoi_polygons_impl(py, tolerance, &clip)
    }
    #[doc = doc_voronoi_edges!(array)]
    #[pyo3(
        signature = (*, tolerance = 0.0, clip = VoronoiClipInput::DEFAULT),
        text_signature = "($self, *, tolerance=0.0, clip='padded')"
    )]
    pub fn voronoi_edges(
        &self,
        py: Python<'_>,
        tolerance: f64,
        clip: VoronoiClipInput,
    ) -> PyResult<crate::py::vectors::Groups> {
        self.voronoi_edges_impl(py, tolerance, &clip)
    }
}

fn require_plain_triangulation_options(
    min_angle: Option<&Bound<'_, PyAny>>,
    max_area: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    if min_angle.is_some() || max_area.is_some() {
        return Err(GeometryError::new_err(
            "min_angle and max_area require method='constrained'",
        ));
    }
    Ok(())
}
