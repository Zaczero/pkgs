#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

unary_shape_method!(
    centroid,
    Centroid,
    doc_centroid,
    crate::dispatch::kernels::unary_centroid,
    synthesized
);
unary_shape_method!(
    point_on_surface,
    PointOnSurface,
    doc_point_on_surface,
    crate::dispatch::kernels::unary_point_on_surface,
    synthesized
);
unary_shape_method!(
    envelope,
    Envelope,
    doc_envelope,
    crate::dispatch::kernels::unary_envelope,
    synthesized
);
unary_shape_method!(
    convex_hull,
    ConvexHull,
    doc_convex_hull,
    crate::dispatch::kernels::unary_convex_hull
);
unary_shape_method!(
    polylabel,
    Polylabel,
    doc_polylabel,
    crate::dispatch::kernels::unary_polylabel,
    tolerance_unit
);
unary_shape_method!(
    maximum_inscribed_circle,
    MaximumInscribedCircle,
    doc_maximum_inscribed_circle,
    crate::dispatch::kernels::unary_maximum_inscribed_circle,
    tolerance_unit
);
unary_shape_method!(
    minimum_bounding_radius,
    MinimumBoundingRadius,
    doc_minimum_bounding_radius,
    crate::dispatch::kernels::unary_minimum_bounding_radius,
    f64_unit
);
unary_shape_method!(
    boundary,
    Boundary,
    doc_boundary,
    crate::dispatch::kernels::unary_boundary
);
unary_shape_method!(
    minimum_clearance,
    MinimumClearance,
    doc_minimum_clearance,
    crate::dispatch::kernels::unary_minimum_clearance,
    f64_unit
);
unary_shape_method!(
    minimum_clearance_line,
    MinimumClearanceLine,
    doc_minimum_clearance_line,
    crate::dispatch::kernels::unary_minimum_clearance_line,
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
        concavity.try_validate(|value| {
            crate::NonNegative::try_new("concavity", value)
                .map(|_| ())
                .map_err(PyErr::from)
        })?;
        let length_threshold =
            length_threshold.resolve_raw(py, "length_threshold", unary_len!(scalar))?;
        length_threshold.try_validate(|value| {
            crate::NonNegative::try_new("length_threshold", value)
                .map(|_| ())
                .map_err(PyErr::from)
        })?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::ConcaveHull,
            unit,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_concave_hull(
                data,
                ctx,
                &concavity,
                &length_threshold
            )
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
            crate::dispatch::Operation::MaximumInscribedRadius,
            unit,
            move |data, ctx| crate::dispatch::kernels::unary_maximum_inscribed_radius(
                data,
                ctx,
                tolerance.as_ref()
            )
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
            crate::dispatch::Operation::MinimumBoundingCircle,
            unit,
            default,
            crate::dispatch::kernels::unary_minimum_bounding_circle
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
            crate::dispatch::kernels::unary_minimum_rotated_rectangle
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
        concavity.try_validate(|value| {
            crate::NonNegative::try_new("concavity", value)
                .map(|_| ())
                .map_err(PyErr::from)
        })?;
        let length_threshold =
            length_threshold.resolve_raw(py, "length_threshold", unary_len!(array, self))?;
        length_threshold.try_validate(|value| {
            crate::NonNegative::try_new("length_threshold", value)
                .map(|_| ())
                .map_err(PyErr::from)
        })?;
        unary_spine_shapes!(
            array,
            py,
            self,
            crate::dispatch::Operation::ConcaveHull,
            unit,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_concave_hull(
                data,
                ctx,
                &concavity,
                &length_threshold
            )
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
            crate::dispatch::Operation::MaximumInscribedRadius,
            unit,
            move |data, ctx| crate::dispatch::kernels::unary_maximum_inscribed_radius(
                data,
                ctx,
                tolerance.as_ref()
            )
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
            crate::dispatch::Operation::MinimumBoundingCircle,
            unit,
            default,
            crate::dispatch::kernels::unary_minimum_bounding_circle
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
            crate::dispatch::kernels::unary_minimum_rotated_rectangle
        )
    }
}
