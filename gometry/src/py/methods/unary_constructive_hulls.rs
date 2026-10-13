use crate::dispatch::{MetricOp, kernels};
use crate::py::methods::unary_constructive_methods::{
    Bound, DefaultedF64Input, DistanceUnit, F64Param, Py, PyAny, PyErr, PyGeometry,
    PyGeometryArray, PyResult, Python, pymethods,
};

unary_shape_method!(
    centroid,
    Centroid,
    doc_centroid,
    kernels::unary_centroid,
    synthesized
);
unary_shape_method!(
    point_on_surface,
    PointOnSurface,
    doc_point_on_surface,
    kernels::unary_point_on_surface,
    synthesized
);
unary_shape_method!(
    envelope,
    Envelope,
    doc_envelope,
    kernels::unary_envelope,
    synthesized
);
unary_shape_method!(
    convex_hull,
    ConvexHull,
    doc_convex_hull,
    kernels::unary_convex_hull,
    kernels::unary_convex_hull_array,
    synthesized
);
unary_shape_method!(
    polylabel,
    (Metric Polylabel),
    doc_polylabel,
    kernels::unary_polylabel,
    tolerance_unit
);
unary_shape_method!(
    maximum_inscribed_circle,
    (Metric MaximumInscribedCircle),
    doc_maximum_inscribed_circle,
    kernels::unary_maximum_inscribed_circle,
    tolerance_unit
);
unary_shape_method!(
    minimum_bounding_radius,
    (Metric MinimumBoundingRadius),
    doc_minimum_bounding_radius,
    kernels::unary_minimum_bounding_radius,
    f64_unit
);
unary_shape_method!(boundary, Boundary, doc_boundary, kernels::unary_boundary);
unary_shape_method!(
    minimum_clearance,
    (Metric MinimumClearance),
    doc_minimum_clearance,
    kernels::unary_minimum_clearance,
    f64_unit
);
unary_shape_method!(
    minimum_clearance_line,
    (Metric MinimumClearanceLine),
    doc_minimum_clearance_line,
    kernels::unary_minimum_clearance_line,
    kernels::unary_minimum_clearance_line_array,
    unit_shape
);

#[pymethods]
impl PyGeometry {
    #[doc = doc_concave_hull!(scalar)]
    #[pyo3(
        signature = (*, concavity = DefaultedF64Input::Default(2.0), length_threshold = DefaultedF64Input::Default(0.0), unit = None),
        text_signature = "($self, *, concavity=2.0, length_threshold=0.0, unit=None)"
    )]
    pub fn concave_hull(
        &self,
        py: Python<'_>,
        concavity: DefaultedF64Input,
        length_threshold: DefaultedF64Input,
        unit: Option<DistanceUnit>,
    ) -> PyResult<crate::Typed> {
        let concavity = concavity.resolve_raw(py, "concavity", unary_len!(scalar))?;
        concavity.require_non_negative("concavity")?;
        let length_threshold =
            length_threshold.resolve_raw(py, "length_threshold", unary_len!(scalar))?;
        length_threshold.require_non_negative("length_threshold")?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::ConcaveHull),
            unit,
            default,
            move |data, ctx| kernels::unary_concave_hull(data, ctx, &concavity, &length_threshold)
        )
    }
    #[doc = doc_maximum_inscribed_radius!(scalar)]
    #[pyo3(
        signature = (*, tolerance = None, unit = None),
        text_signature = "($self, *, tolerance=None, unit=None)"
    )]
    pub fn maximum_inscribed_radius(
        &self,
        py: Python<'_>,
        tolerance: Option<&Bound<'_, PyAny>>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<f64> {
        let tolerance = tolerance
            .map(|value| F64Param::parse(value, "tolerance", unary_len!(scalar)))
            .transpose()?;
        unary_spine_f64!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::MaximumInscribedRadius),
            unit,
            move |data, ctx| kernels::unary_maximum_inscribed_radius(data, ctx, tolerance.as_ref())
        )
    }
    #[doc = doc_minimum_bounding_circle!(scalar)]
    #[pyo3(signature = (*, unit = None))]
    pub fn minimum_bounding_circle(
        &self,
        py: Python<'_>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<crate::Typed> {
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::MinimumBoundingCircle),
            unit,
            default,
            kernels::unary_minimum_bounding_circle
        )
    }
    #[doc = doc_minimum_rotated_rectangle!(scalar)]
    pub fn minimum_rotated_rectangle(&self, py: Python<'_>) -> PyResult<crate::Typed> {
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::MinimumRotatedRectangle,
            None,
            default,
            kernels::unary_minimum_rotated_rectangle
        )
    }
}

#[pymethods]
impl PyGeometryArray {
    #[doc = doc_concave_hull!(array)]
    #[pyo3(
        signature = (*, concavity = DefaultedF64Input::Default(2.0), length_threshold = DefaultedF64Input::Default(0.0), unit = None),
        text_signature = "($self, *, concavity=2.0, length_threshold=0.0, unit=None)"
    )]
    pub fn concave_hull(
        &self,
        py: Python<'_>,
        concavity: DefaultedF64Input,
        length_threshold: DefaultedF64Input,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        let concavity = concavity.resolve_raw(py, "concavity", unary_len!(array, self))?;
        concavity.require_non_negative("concavity")?;
        let length_threshold =
            length_threshold.resolve_raw(py, "length_threshold", unary_len!(array, self))?;
        length_threshold.require_non_negative("length_threshold")?;
        unary_spine_shapes!(
            array,
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::ConcaveHull),
            unit,
            default,
            move |data, ctx| kernels::unary_concave_hull(data, ctx, &concavity, &length_threshold)
        )
    }
    #[doc = doc_maximum_inscribed_radius!(array)]
    #[pyo3(
        signature = (*, tolerance = None, unit = None),
        text_signature = "($self, *, tolerance=None, unit=None)"
    )]
    pub fn maximum_inscribed_radius(
        &self,
        py: Python<'_>,
        tolerance: Option<&Bound<'_, PyAny>>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        let tolerance = tolerance
            .map(|value| F64Param::parse(value, "tolerance", unary_len!(array, self)))
            .transpose()?;
        unary_spine_f64!(
            array,
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::MaximumInscribedRadius),
            unit,
            move |data, ctx| kernels::unary_maximum_inscribed_radius(data, ctx, tolerance.as_ref())
        )
    }
    #[doc = doc_minimum_bounding_circle!(array)]
    #[pyo3(signature = (*, unit = None))]
    pub fn minimum_bounding_circle(
        &self,
        py: Python<'_>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        unary_spine_shapes!(
            array,
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::MinimumBoundingCircle),
            unit,
            default,
            kernels::unary_minimum_bounding_circle
        )
    }
    #[doc = doc_minimum_rotated_rectangle!(array)]
    pub fn minimum_rotated_rectangle(&self, py: Python<'_>) -> PyResult<Self> {
        unary_spine_shapes!(
            array,
            py,
            self,
            crate::dispatch::Operation::MinimumRotatedRectangle,
            None,
            default,
            kernels::unary_minimum_rotated_rectangle_array
        )
    }
}
