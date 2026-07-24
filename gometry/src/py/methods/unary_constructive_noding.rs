#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

unary_shape_method!(
    build_area,
    BuildArea,
    doc_build_area,
    crate::dispatch::kernels::unary_build_area,
    synthesized
);
unary_shape_method!(
    node,
    Node,
    doc_node,
    crate::dispatch::kernels::unary_node,
    synthesized
);
unary_shape_method!(
    unique_points,
    UniquePoints,
    doc_unique_points,
    crate::dispatch::kernels::unary_unique_points
);
unary_shape_method!(
    split_antimeridian,
    SplitAntimeridian,
    doc_split_antimeridian,
    crate::dispatch::kernels::unary_split_antimeridian
);

#[pymethods]
impl PyGeometry {
    #[doc = doc_polygonize!(scalar)]
    pub fn polygonize(&self) -> PyResult<PyGeometryArray> {
        self.polygonize_impl()
    }
    #[doc = doc_line_merge!(scalar)]
    pub fn line_merge(&self) -> PyResult<crate::Typed> {
        self.line_merge_impl()
    }
    #[doc = doc_extremes!(scalar)]
    pub fn extremes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.extremes_impl(py)
    }
    #[doc = doc_self_intersections!(scalar)]
    pub fn self_intersections(&self) -> PyGeometryArray {
        self.self_intersections_impl()
    }
}

#[pymethods]
impl PyGeometryArray {
    #[doc = doc_polygonize!(array)]
    pub fn polygonize(&self, py: Python<'_>) -> PyResult<crate::py::vectors::Groups> {
        self.polygonize_impl(py)
    }
    #[doc = doc_line_merge!(array)]
    pub fn line_merge(&self, py: Python<'_>) -> PyResult<Self> {
        self.line_merge_impl(py)
    }
    #[doc = doc_extremes!(array)]
    pub fn extremes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.extremes_impl(py)
    }
    #[doc = doc_self_intersections!(array)]
    pub fn self_intersections(&self) -> PyResult<crate::py::vectors::Groups> {
        self.self_intersections_impl()
    }
}
