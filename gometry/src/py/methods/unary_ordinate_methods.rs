#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::{DefaultedF64Input, PyGeometry, PyGeometryArray};

method_doc! {
    doc_force_2d;
    body: r"Make each geometry planar, dropping any Z and M ordinates. Returns pure XY
of the same geometry type and CRS. Already-2D input is returned unchanged.
The one obvious way to flatten.";
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
that already carry Z keep it; M passes through. The one obvious way to lift
to 3D.

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
    tail: r"See Also
--------
force_2d : Drop Z and M to plain XY.
set_z : Set the Z ordinate at every vertex (overwriting existing Z).

Examples
--------
>>> import gometry as gm
>>> arr = gm.GeometryArray([gm.Point(1, 2)])
>>> arr.force_3d().to_wkt()[0]
'POINT Z (1 2 0)'";
}

method_doc! {
    doc_set_z;
    body: r"Set the Z ordinate at every vertex, or remove it. A numeric ``z`` writes
that Z at every vertex (replacing any existing Z); ``None`` removes the Z
ordinate. M passes through unchanged. To fill only the vertices that lack Z,
use force_3d; to drop to XY, use force_2d.

Parameters
----------
z : float or None
    Finite Z to assign at every vertex, or ``None`` to remove the Z ordinate.";
    scalar: r"

Returns
-------
Geometry
    The geometry with the new Z ordinate applied (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    One result per row (kinds preserved).

";
    tail: r"See Also
--------
set_m : Set or clear the M ordinate.
force_3d : Fill only the vertices that lack Z.

Examples
--------
>>> import gometry as gm
>>> arr = gm.GeometryArray([gm.Point(1, 2)])
>>> arr.set_z(30.0).to_wkt()[0]
'POINT Z (1 2 30)'";
}

method_doc! {
    doc_set_m;
    body: r"Set the M ordinate at every vertex, or remove it. A numeric ``m`` writes
that M at every vertex (replacing any existing M); ``None`` removes the M
ordinate. Z passes through unchanged.

Parameters
----------
m : float or None
    Finite M to assign at every vertex, or ``None`` to remove the M ordinate.";
    scalar: r"

Returns
-------
Geometry
    The geometry with the new M ordinate applied (same kind as the input).

";
    array: r"

Returns
-------
GeometryArray
    One result per row (kinds preserved).

";
    tail: r"See Also
--------
set_z : Set or clear the Z ordinate.
interpolate_m : Assign M by interpolating along the linework.

Examples
--------
>>> import gometry as gm
>>> arr = gm.GeometryArray([gm.Point(1, 2)])
>>> arr.set_m(5.0).to_wkt()[0]
'POINT M (1 2 5)'";
}

unary_shape_method!(
    force_2d,
    Force2d,
    doc_force_2d,
    crate::dispatch::kernels::unary_force_2d
);

#[pymethods]
impl PyGeometry {
    #[doc = doc_force_3d!(scalar)]
    #[pyo3(signature = (z = DefaultedF64Input::Default(0.0)))]
    pub fn force_3d(&self, py: Python<'_>, z: DefaultedF64Input) -> PyResult<crate::Typed> {
        let z = z.resolve_coordinate_scalar(py, "z")?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Force3d,
            None,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_force_3d(data, ctx, z)
        )
    }

    #[doc = doc_set_z!(scalar)]
    #[pyo3(signature = (z))]
    pub fn set_z(&self, py: Python<'_>, z: Option<&Bound<'_, PyAny>>) -> PyResult<crate::Typed> {
        let z = z
            .map(|value| crate::finite_coordinate_required("z", value))
            .transpose()?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::SetZ,
            None,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_set_z(data, ctx, z, true)
        )
    }

    #[doc = doc_set_m!(scalar)]
    #[pyo3(signature = (m))]
    pub fn set_m(&self, py: Python<'_>, m: Option<&Bound<'_, PyAny>>) -> PyResult<crate::Typed> {
        let m = m
            .map(|value| crate::finite_coordinate_required("m", value))
            .transpose()?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::SetM,
            None,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_set_m(data, ctx, m, true)
        )
    }
}

#[pymethods]
impl PyGeometryArray {
    #[doc = doc_force_3d!(array)]
    #[pyo3(signature = (z = DefaultedF64Input::Default(0.0)))]
    pub fn force_3d(&self, py: Python<'_>, z: DefaultedF64Input) -> PyResult<Self> {
        let z = z.resolve_coordinate_scalar(py, "z")?;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::Force3d,
            None,
            crate::dispatch::PackedUnary::Force3d { z_fill: z },
            move |data, ctx| crate::dispatch::kernels::unary_force_3d(data, ctx, z)
        )
    }

    #[doc = doc_set_z!(array)]
    #[pyo3(signature = (z))]
    pub fn set_z(&self, py: Python<'_>, z: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let z = z
            .map(|value| crate::finite_coordinate_required("z", value))
            .transpose()?;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::SetZ,
            None,
            crate::dispatch::PackedUnary::SetOrdinate {
                value: z,
                overwrite: true,
            },
            move |data, ctx| crate::dispatch::kernels::unary_set_z(data, ctx, z, true)
        )
    }

    #[doc = doc_set_m!(array)]
    #[pyo3(signature = (m))]
    pub fn set_m(&self, py: Python<'_>, m: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let m = m
            .map(|value| crate::finite_coordinate_required("m", value))
            .transpose()?;
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            crate::dispatch::Operation::SetM,
            None,
            crate::dispatch::PackedUnary::SetOrdinate {
                value: m,
                overwrite: true,
            },
            move |data, ctx| crate::dispatch::kernels::unary_set_m(data, ctx, m, true)
        )
    }
}
