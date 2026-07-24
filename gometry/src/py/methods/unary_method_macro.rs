//! Shared `macro_rules!` helpers for scalar/`GeometryArray` unary-method dedup.

/// Row count for per-element float parameter parsing.
macro_rules! unary_len {
    (scalar) => {
        1_usize
    };
    (array, $self:expr) => {
        $self.storage().len()
    };
}

/// Geometry-returning unary spine (`unary_scalar_shape` / `unary_array_shapes`).
macro_rules! unary_spine_shapes {
    (
        scalar,
        $py:expr,
        $self:expr,
        $op:expr,
        $unit:expr,
        default,
        $kernel:expr
    ) => {
        crate::dispatch::unary_scalar_shape($py, $self, $op, $unit, $kernel)
    };
    (
        array,
        $py:expr,
        $self:expr,
        $op:expr,
        $unit:expr,
        default,
        $kernel:expr
    ) => {
        crate::dispatch::unary_array_shapes($py, $self, $op, $unit, None, $kernel)
    };
    (
        array,
        $py:expr,
        $self:expr,
        $op:expr,
        $unit:expr,
        synthesized,
        $kernel:expr
    ) => {
        crate::dispatch::unary_array_shapes(
            $py,
            $self,
            $op,
            $unit,
            Some(&crate::dispatch::PackedUnary::Synthesized),
            $kernel,
        )
    };
}

/// Numeric-returning unary spine (`unary_scalar` / `unary_array`).
macro_rules! unary_spine_f64 {
    (
        scalar,
        $py:expr,
        $self:expr,
        $op:expr,
        $unit:expr,
        $kernel:expr
    ) => {
        crate::dispatch::unary_scalar($py, $self, $op, $unit, $kernel)
    };
    (
        array,
        $py:expr,
        $self:expr,
        $op:expr,
        $unit:expr,
        $kernel:expr
    ) => {
        crate::dispatch::unary_array($py, $self, $op, $unit, None, $kernel)
    };
}

/// Geometry-returning unary spine with an explicit packed fast-path variant (array only).
macro_rules! unary_spine_shapes_extras {
    (
        array,
        $py:expr,
        $self:expr,
        $op:expr,
        $unit:expr,
        $packed:expr,
        $kernel:expr
    ) => {
        crate::dispatch::unary_array_shapes($py, $self, $op, $unit, Some(&$packed), $kernel)
    };
}

/// Emit the identical scalar/`GeometryArray` PyO3 shells for simple unary methods.
macro_rules! unary_shape_method {
    ($name:ident, $op:ident, $doc:ident, $kernel:path) => {
        #[pymethods]
        impl PyGeometry {
            #[doc = $doc!(scalar)]
            pub fn $name(&self, py: Python<'_>) -> PyResult<crate::Typed> {
                unary_spine_shapes!(
                    scalar,
                    py,
                    self,
                    crate::dispatch::Operation::$op,
                    None,
                    default,
                    $kernel
                )
            }
        }

        #[pymethods]
        impl PyGeometryArray {
            #[doc = $doc!(array)]
            pub fn $name(&self, py: Python<'_>) -> PyResult<Self> {
                unary_spine_shapes!(
                    array,
                    py,
                    self,
                    crate::dispatch::Operation::$op,
                    None,
                    default,
                    $kernel
                )
            }
        }
    };
    ($name:ident, $op:ident, $doc:ident, $kernel:path, synthesized) => {
        #[pymethods]
        impl PyGeometry {
            #[doc = $doc!(scalar)]
            pub fn $name(&self, py: Python<'_>) -> PyResult<crate::Typed> {
                unary_spine_shapes!(
                    scalar,
                    py,
                    self,
                    crate::dispatch::Operation::$op,
                    None,
                    default,
                    $kernel
                )
            }
        }

        #[pymethods]
        impl PyGeometryArray {
            #[doc = $doc!(array)]
            pub fn $name(&self, py: Python<'_>) -> PyResult<Self> {
                unary_spine_shapes!(
                    array,
                    py,
                    self,
                    crate::dispatch::Operation::$op,
                    None,
                    synthesized,
                    $kernel
                )
            }
        }
    };
    ($name:ident, $op:ident, $doc:ident, $kernel:path, unit_shape) => {
        #[pymethods]
        impl PyGeometry {
            #[doc = $doc!(scalar)]
            #[pyo3(signature = (*, unit = None))]
            pub fn $name(
                &self,
                py: Python<'_>,
                unit: Option<DistanceUnit>,
            ) -> PyResult<crate::Typed> {
                unary_spine_shapes!(
                    scalar,
                    py,
                    self,
                    crate::dispatch::Operation::$op,
                    unit,
                    default,
                    $kernel
                )
            }
        }

        #[pymethods]
        impl PyGeometryArray {
            #[doc = $doc!(array)]
            #[pyo3(signature = (*, unit = None))]
            pub fn $name(&self, py: Python<'_>, unit: Option<DistanceUnit>) -> PyResult<Self> {
                unary_spine_shapes!(
                    array,
                    py,
                    self,
                    crate::dispatch::Operation::$op,
                    unit,
                    default,
                    $kernel
                )
            }
        }
    };
    ($name:ident, $op:ident, $doc:ident, $kernel:path, f64_unit) => {
        #[pymethods]
        impl PyGeometry {
            #[doc = $doc!(scalar)]
            #[pyo3(signature = (*, unit = None))]
            pub fn $name(&self, py: Python<'_>, unit: Option<DistanceUnit>) -> PyResult<f64> {
                unary_spine_f64!(
                    scalar,
                    py,
                    self,
                    crate::dispatch::Operation::$op,
                    unit,
                    $kernel
                )
            }
        }

        #[pymethods]
        impl PyGeometryArray {
            #[doc = $doc!(array)]
            #[pyo3(signature = (*, unit = None))]
            pub fn $name(&self, py: Python<'_>, unit: Option<DistanceUnit>) -> PyResult<Py<PyAny>> {
                unary_spine_f64!(
                    array,
                    py,
                    self,
                    crate::dispatch::Operation::$op,
                    unit,
                    $kernel
                )
            }
        }
    };
    ($name:ident, $op:ident, $doc:ident, $kernel:path, tolerance_unit) => {
        #[pymethods]
        impl PyGeometry {
            #[doc = $doc!(scalar)]
            #[pyo3(signature = (*, tolerance = None, unit = None), text_signature = "($self, *, tolerance=None, unit=None)")]
            pub fn $name(
                &self,
                py: Python<'_>,
                tolerance: Option<&Bound<'_, PyAny>>,
                unit: Option<DistanceUnit>,
            ) -> PyResult<crate::Typed> {
                let tolerance = tolerance
                    .map(|value| F64Param::parse_raw(value, "tolerance", unary_len!(scalar)))
                    .transpose()?;
                if let Some(tolerance) = &tolerance {
                    tolerance.try_validate(|value| {
                        crate::Positive::try_new("tolerance", value)
                            .map(|_| ())
                            .map_err(PyErr::from)
                    })?;
                }
                unary_spine_shapes!(
                    scalar,
                    py,
                    self,
                    crate::dispatch::Operation::$op,
                    unit,
                    default,
                    move |data, ctx| $kernel(data, ctx, tolerance.as_ref())
                )
            }
        }

        #[pymethods]
        impl PyGeometryArray {
            #[doc = $doc!(array)]
            #[pyo3(signature = (*, tolerance = None, unit = None), text_signature = "($self, *, tolerance=None, unit=None)")]
            pub fn $name(
                &self,
                py: Python<'_>,
                tolerance: Option<&Bound<'_, PyAny>>,
                unit: Option<DistanceUnit>,
            ) -> PyResult<Self> {
                let tolerance = tolerance
                    .map(|value| F64Param::parse_raw(value, "tolerance", unary_len!(array, self)))
                    .transpose()?;
                if let Some(tolerance) = &tolerance {
                    tolerance.try_validate(|value| {
                        crate::Positive::try_new("tolerance", value)
                            .map(|_| ())
                            .map_err(PyErr::from)
                    })?;
                }
                unary_spine_shapes!(
                    array,
                    py,
                    self,
                    crate::dispatch::Operation::$op,
                    unit,
                    synthesized,
                    move |data, ctx| $kernel(data, ctx, tolerance.as_ref())
                )
            }
        }
    };
}
