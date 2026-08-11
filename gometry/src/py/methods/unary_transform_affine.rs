#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::py::methods::unary_transform_methods::{
    Bound, GeometryArrayStorage, OriginSpec, PyAny, PyGeometry, PyGeometryArray, PyResult, Python,
    affine_about, angle_radians, finite_f64_required, parse_affine_matrix, parse_precision,
    pymethods,
};

unary_shape_method!(
    reverse,
    Reverse,
    doc_reverse,
    crate::dispatch::kernels::unary_reverse
);
unary_shape_method!(
    swap_xy,
    SwapXy,
    doc_swap_xy,
    crate::dispatch::kernels::unary_swap_xy
);

#[pymethods]
impl PyGeometry {
    #[doc = doc_normalize!(scalar)]
    pub fn normalize(&self, py: Python<'_>) -> PyResult<crate::Typed> {
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Normalize,
            None,
            default,
            crate::dispatch::kernels::unary_normalize
        )
    }

    #[doc = doc_orient_polygons!(scalar)]
    #[pyo3(signature = (*, ccw = true))]
    pub fn orient_polygons(&self, py: Python<'_>, ccw: bool) -> PyResult<crate::Typed> {
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::OrientPolygons,
            None,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_orient_polygons(data, ctx, ccw)
        )
    }

    #[doc = doc_quantize!(scalar)]
    pub fn quantize(&self, py: Python<'_>, precision: &Bound<'_, PyAny>) -> PyResult<crate::Typed> {
        let precision = parse_precision(precision)?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Quantize,
            None,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_quantize(data, ctx, precision)
        )
    }

    #[doc = doc_affine_transform!(scalar)]
    pub fn affine_transform(
        &self,
        py: Python<'_>,
        matrix: &Bound<'_, PyAny>,
    ) -> PyResult<crate::Typed> {
        let matrix = parse_affine_matrix(matrix)?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::AffineTransform,
            None,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_affine_transform(data, ctx, &matrix)
        )
    }

    #[doc = doc_translate!(scalar)]
    pub fn translate(
        &self,
        py: Python<'_>,
        x_offset: &Bound<'_, PyAny>,
        y_offset: &Bound<'_, PyAny>,
    ) -> PyResult<crate::Typed> {
        let xoff = finite_f64_required("x_offset", x_offset)?;
        let yoff = finite_f64_required("y_offset", y_offset)?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Translate,
            None,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_translate(data, ctx, xoff, yoff)
        )
    }

    #[doc = doc_rotate!(scalar)]
    #[pyo3(
        signature = (angle, *, origin = OriginSpec::Centroid, radians = false),
        text_signature = "($self, angle, *, origin='centroid', radians=False)"
    )]
    pub fn rotate(
        &self,
        py: Python<'_>,
        angle: &Bound<'_, PyAny>,
        origin: OriginSpec,
        radians: bool,
    ) -> PyResult<crate::Typed> {
        let angle = angle_radians("angle", angle, radians)?;
        let spec = origin;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Rotate,
            None,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_rotate(data, ctx, angle, spec)
        )
    }

    #[doc = doc_scale!(scalar)]
    #[pyo3(
        signature = (x_factor, y_factor = None, *, origin = OriginSpec::Centroid),
        text_signature = "($self, x_factor, y_factor=None, *, origin='centroid')"
    )]
    pub fn scale(
        &self,
        py: Python<'_>,
        x_factor: &Bound<'_, PyAny>,
        y_factor: Option<&Bound<'_, PyAny>>,
        origin: OriginSpec,
    ) -> PyResult<crate::Typed> {
        let xfact = finite_f64_required("x_factor", x_factor)?;
        let yfact = match y_factor {
            Some(y_factor) => finite_f64_required("y_factor", y_factor)?,
            None => xfact,
        };
        let spec = origin;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Scale,
            None,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_scale(data, ctx, xfact, yfact, spec)
        )
    }

    #[doc = doc_skew!(scalar)]
    #[pyo3(
        signature = (x_angle = 0.0, y_angle = 0.0, *, origin = OriginSpec::Centroid, radians = false),
        text_signature = "($self, x_angle=0.0, y_angle=0.0, *, origin='centroid', radians=False)"
    )]
    pub fn skew(
        &self,
        py: Python<'_>,
        x_angle: f64,
        y_angle: f64,
        origin: OriginSpec,
        radians: bool,
    ) -> PyResult<crate::Typed> {
        let xs = if radians {
            x_angle
        } else {
            x_angle.to_radians()
        };
        let ys = if radians {
            y_angle
        } else {
            y_angle.to_radians()
        };
        let spec = origin;
        let tan_x = xs.tan();
        let tan_y = ys.tan();
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Skew,
            None,
            default,
            move |data, ctx| {
                let origin = spec.resolve_shape(data.shape())?;
                let seated = affine_about(1.0, tan_x, tan_y, 1.0, origin);
                crate::dispatch::kernels::unary_affine_transform(data, ctx, &seated)
            }
        )
    }
}

#[pymethods]
impl PyGeometryArray {
    #[doc = doc_normalize!(array)]
    pub fn normalize(&self, py: Python<'_>) -> PyResult<Self> {
        if !matches!(self.storage(), GeometryArrayStorage::Mixed(_)) {
            return Ok(self.normalize_unary_packed(py));
        }
        unary_spine_shapes!(
            array,
            py,
            self,
            crate::dispatch::Operation::Normalize,
            None,
            default,
            crate::dispatch::kernels::unary_normalize
        )
    }

    #[doc = doc_orient_polygons!(array)]
    #[pyo3(signature = (*, ccw = true))]
    pub fn orient_polygons(&self, py: Python<'_>, ccw: bool) -> PyResult<Self> {
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::OrientPolygons,
            None,
            crate::dispatch::PackedUnary::OrientPolygons { ccw },
            move |data, ctx| crate::dispatch::kernels::unary_orient_polygons(data, ctx, ccw)
        )
    }

    #[doc = doc_quantize!(array)]
    pub fn quantize(&self, py: Python<'_>, precision: &Bound<'_, PyAny>) -> PyResult<Self> {
        let precision = parse_precision(precision)?;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::Quantize,
            None,
            crate::dispatch::PackedUnary::Quantize { precision },
            move |data, ctx| crate::dispatch::kernels::unary_quantize(data, ctx, precision)
        )
    }

    #[doc = doc_affine_transform!(array)]
    pub fn affine_transform(&self, py: Python<'_>, matrix: &Bound<'_, PyAny>) -> PyResult<Self> {
        let matrix = parse_affine_matrix(matrix)?;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::AffineTransform,
            None,
            crate::dispatch::PackedUnary::Affine { matrix },
            move |data, ctx| crate::dispatch::kernels::unary_affine_transform(data, ctx, &matrix)
        )
    }

    #[doc = doc_translate!(array)]
    pub fn translate(
        &self,
        py: Python<'_>,
        x_offset: &Bound<'_, PyAny>,
        y_offset: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let xoff = finite_f64_required("x_offset", x_offset)?;
        let yoff = finite_f64_required("y_offset", y_offset)?;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::Translate,
            None,
            crate::dispatch::PackedUnary::Affine {
                matrix: [1.0, 0.0, 0.0, 1.0, xoff, yoff],
            },
            move |data, ctx| crate::dispatch::kernels::unary_translate(data, ctx, xoff, yoff)
        )
    }

    #[doc = doc_rotate!(array)]
    #[pyo3(
        signature = (angle, *, origin = OriginSpec::Centroid, radians = false),
        text_signature = "($self, angle, *, origin='centroid', radians=False)"
    )]
    pub fn rotate(
        &self,
        py: Python<'_>,
        angle: &Bound<'_, PyAny>,
        origin: OriginSpec,
        radians: bool,
    ) -> PyResult<Self> {
        let angle = angle_radians("angle", angle, radians)?;
        let spec = origin;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::Rotate,
            None,
            crate::dispatch::PackedUnary::Rotate {
                origin: spec,
                angle_radians: angle,
            },
            move |data, ctx| crate::dispatch::kernels::unary_rotate(data, ctx, angle, spec)
        )
    }

    #[doc = doc_scale!(array)]
    #[pyo3(
        signature = (x_factor, y_factor = None, *, origin = OriginSpec::Centroid),
        text_signature = "($self, x_factor, y_factor=None, *, origin='centroid')"
    )]
    pub fn scale(
        &self,
        py: Python<'_>,
        x_factor: &Bound<'_, PyAny>,
        y_factor: Option<&Bound<'_, PyAny>>,
        origin: OriginSpec,
    ) -> PyResult<Self> {
        let xfact = finite_f64_required("x_factor", x_factor)?;
        let yfact = match y_factor {
            Some(y_factor) => finite_f64_required("y_factor", y_factor)?,
            None => xfact,
        };
        let spec = origin;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::Scale,
            None,
            crate::dispatch::PackedUnary::Scale {
                origin: spec,
                xfact,
                yfact,
            },
            move |data, ctx| crate::dispatch::kernels::unary_scale(data, ctx, xfact, yfact, spec)
        )
    }

    #[doc = doc_skew!(array)]
    #[pyo3(
        signature = (x_angle = 0.0, y_angle = 0.0, *, origin = OriginSpec::Centroid, radians = false),
        text_signature = "($self, x_angle=0.0, y_angle=0.0, *, origin='centroid', radians=False)"
    )]
    pub fn skew(
        &self,
        py: Python<'_>,
        x_angle: f64,
        y_angle: f64,
        origin: OriginSpec,
        radians: bool,
    ) -> PyResult<Self> {
        let xs = if radians {
            x_angle
        } else {
            x_angle.to_radians()
        };
        let ys = if radians {
            y_angle
        } else {
            y_angle.to_radians()
        };
        let spec = origin;
        let tan_x = xs.tan();
        let tan_y = ys.tan();
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::Skew,
            None,
            crate::dispatch::PackedUnary::Skew {
                origin: spec,
                tan_x,
                tan_y,
            },
            move |data, ctx| {
                let origin = spec.resolve_shape(data.shape())?;
                let seated = affine_about(1.0, tan_x, tan_y, 1.0, origin);
                crate::dispatch::kernels::unary_affine_transform(data, ctx, &seated)
            }
        )
    }
}
