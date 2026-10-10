use crate::py::errors::GeometryError;
use crate::py::methods::unary_constructive_methods::{
    Bound, PyAny, PyGeometry, PyGeometryArray, PyResult, Python, pymethods,
};
use crate::py::support::{TriangulationMethod, VoronoiClipInput, VoronoiOutput};
use crate::py::vectors::Groups;

#[pymethods]
impl PyGeometry {
    #[doc = doc_triangulate!(scalar)]
    #[pyo3(
        signature = (*, method, min_angle = None, max_area = None)
    )]
    pub fn triangulate(
        &self,
        py: Python<'_>,
        method: TriangulationMethod,
        min_angle: Option<&Bound<'_, PyAny>>,
        max_area: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyGeometryArray> {
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
    #[doc = doc_sample_points!(scalar)]
    #[pyo3(signature = (count, *, seed))]
    pub fn sample_points(
        &self,
        py: Python<'_>,
        count: &Bound<'_, PyAny>,
        seed: &Bound<'_, PyAny>,
    ) -> PyResult<PyGeometryArray> {
        self.sample_points_impl(py, count, seed)
    }
    #[doc = doc_voronoi!(scalar)]
    #[pyo3(
        signature = (*, output = VoronoiOutput::Polygons, tolerance = 0.0, clip = VoronoiClipInput::DEFAULT),
        text_signature = "($self, *, output='polygons', tolerance=0.0, clip='padded')"
    )]
    pub fn voronoi(
        &self,
        py: Python<'_>,
        output: VoronoiOutput,
        tolerance: f64,
        clip: VoronoiClipInput,
    ) -> PyResult<PyGeometryArray> {
        self.voronoi_impl(py, output, tolerance, &clip)
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
    ) -> PyResult<Groups> {
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
    ) -> PyResult<Groups> {
        self.sample_points_impl(count, seed)
    }
    #[doc = doc_voronoi!(array)]
    #[pyo3(
        signature = (*, output = VoronoiOutput::Polygons, tolerance = 0.0, clip = VoronoiClipInput::DEFAULT),
        text_signature = "($self, *, output='polygons', tolerance=0.0, clip='padded')"
    )]
    pub fn voronoi(
        &self,
        py: Python<'_>,
        output: VoronoiOutput,
        tolerance: f64,
        clip: VoronoiClipInput,
    ) -> PyResult<Groups> {
        self.voronoi_impl(py, output, tolerance, &clip)
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
