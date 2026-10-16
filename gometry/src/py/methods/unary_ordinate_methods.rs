use pyo3::prelude::*;

use crate::dispatch::kernels;
use crate::{DefaultedF64Input, PyGeometry, PyGeometryArray};

method_doc! {
    doc_force_2d;
    body: r"Drop Z and M ordinates, preserving geometry type and CRS.
Already-2D input is returned unchanged.";
    scalar: r"

Returns
-------
Geometry
    The XY-only geometry (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    One XY-only geometry per row (kinds preserved).

";
    tail: r"See Also
--------
force_3d : Add a Z ordinate, filling missing vertices.

Examples
--------
>>> import gometry as gm
>>> gm.from_wkt('POINT Z (1 2 3)').force_2d().to_wkt()
'POINT (1 2)'";
}

method_doc! {
    doc_force_3d;
    body: r"Make each geometry 3D, filling vertices that lack Z with ``z``. Vertices
that already carry Z keep it; M passes through.

Parameters
----------
z : float, default 0.0
    Finite Z to assign where it is missing.";
    scalar: r"

Returns
-------
Geometry
    The geometry with a Z ordinate on every vertex (same kind as the
    input).

";
    array: r"

Returns
-------
GeometryArray
    One 3D geometry per row (kinds preserved).

";
    tail: r"Raises
------
InvalidGeometryError
    If ``z`` is not finite.

See Also
--------
force_2d : Drop Z and M to plain XY.
with_coordinates : Replace individual coordinate ordinates.

Examples
--------
>>> import gometry as gm
>>> arr = gm.GeometryArray([gm.Point(1, 2)])
>>> arr.force_3d().to_wkt()[0]
'POINT Z (1 2 0)'";
}

unary_shape_method!(force_2d, Force2d, doc_force_2d, kernels::unary_force_2d);

#[pymethods]
impl PyGeometry {
    #[doc = doc_force_3d!(scalar)]
    #[pyo3(
        signature = (z = DefaultedF64Input::Default(0.0)),
        text_signature = "($self, z=0.0)"
    )]
    pub fn force_3d(&self, py: Python<'_>, z: DefaultedF64Input) -> PyResult<crate::Typed> {
        let z = z.resolve_coordinate_scalar(py, "z")?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Force3d,
            None,
            default,
            move |data, ctx| kernels::unary_force_3d(data, ctx, z)
        )
    }
}

#[pymethods]
impl PyGeometryArray {
    #[doc = doc_force_3d!(array)]
    #[pyo3(
        signature = (z = DefaultedF64Input::Default(0.0)),
        text_signature = "($self, z=0.0)"
    )]
    pub fn force_3d(&self, py: Python<'_>, z: DefaultedF64Input) -> PyResult<Self> {
        let z = z.resolve_coordinate_scalar(py, "z")?;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::Force3d,
            None,
            crate::dispatch::PackedUnary::Force3d { z_fill: z },
            move |data, ctx| kernels::unary_force_3d(data, ctx, z)
        )
    }
}
