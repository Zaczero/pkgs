#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::py::errors::GeometryError;
use crate::py::support::LineReferenceBasis;
use crate::{
    DistanceUnit, F64Param, GeometryInput, PyGeometry, PyGeometryArray, Typed,
    parse_interpolate_plan,
};

/// `basis='m'` forbids distance-only options (`normalized`, `unit`).
fn reject_m_basis_distance_options(normalized: bool, unit: Option<DistanceUnit>) -> PyResult<()> {
    if normalized || unit.is_some() {
        return Err(GeometryError::new_err(
            "normalized and unit require basis='distance'",
        ));
    }
    Ok(())
}

macro_rules! doc_line_interpolate {
    (scalar) => {
        concat!(doc_line_interpolate!(@pre), r"
count : int, optional
    Number of evenly spaced distance-basis samples (``>= 1``). Mutually
    exclusive with ``at`` and unavailable with ``basis='m'``.
", doc_line_interpolate!(@post), r"

Returns
-------
Point or GeometryArray[Point]
    One point for scalar ``at``; a point column for many ``at`` values or
    ``count`` samples.

", doc_line_interpolate!(@tail))
    };
    (array) => {
        concat!(doc_line_interpolate!(@pre), r"
count : int or iterable of int, optional
    Number of evenly spaced distance-basis samples per row (``>= 1``). A scalar
    broadcasts; otherwise pass one count per row. Mutually exclusive with
    ``at`` and unavailable with ``basis='m'``.
", doc_line_interpolate!(@post), r"

Returns
-------
GeometryArray[Point] or Groups[GeometryArray[Point]]
    One point per row for scalar or row-aligned ``at``; one point group per
    row for ``count`` samples.

", doc_line_interpolate!(@tail))
    };
    (@pre) => {
        r"Interpolate point locations along linework.

Parameters
----------
at : float or sequence of float, optional
    One location or many explicit distance-basis locations. Under
    ``basis='m'``, pass one stored M value (or one value per array row).

"
    };
    (@post) => {
        r"
basis : {'distance', 'm'}, default 'distance'
    Use CRS-aware distance, or the line's monotonic M ordinate.

normalized : bool, default False
    Interpret distance-basis ``at`` values as fractions in [0, 1]. Invalid
    with ``basis='m'``.

unit : {'planar', 'meters'}, default None
    Distance-basis unit override. Omitted follows the CRS; invalid with
    ``basis='m'``."
    };
    (@tail) => {
        r"
Raises
------
GeometryTypeError
    If the geometry is not lineal.
InvalidGeometryError
    If the linework is empty, or M values are missing or non-monotonic.
GeometryError
    If input forms conflict, a value is non-finite, or a distance-only option
    is used with ``basis='m'``.

See Also
--------
line_locate : Project a geometry onto the line (inverse of interpolate).
line_substring : Extract a contiguous portion of the line.
interpolate_m : Assign M ordinates along arc length.

Examples
--------
>>> import gometry as gm
>>> line = gm.LineString([(0, 0), (10, 0)])
>>> line.line_interpolate(4).to_wkt()
'POINT (4 0)'"
    };
}

macro_rules! doc_line_substring {
    (scalar) => {
        concat!(doc_line_substring!(@body), r"

Returns
-------
LineString or Point
    The substring (a ``Point`` when ``start == end``).

", doc_line_substring!(@tail))
    };
    (array) => {
        concat!(doc_line_substring!(@body), r"

Returns
-------
GeometryArray
    One substring per row.

", doc_line_substring!(@tail))
    };
    (@body) => {
        r"Return the portion of linework from ``start`` through ``end``.

Parameters
----------
start, end : float or sequence of float
    Ordered locations on the selected basis. Distance values follow the CRS;
    M values are stored route measures. A scalar applies to every array row.

basis : {'distance', 'm'}, default 'distance'
    Use CRS-aware distance, or the line's monotonic M ordinate.

normalized : bool, default False
    Interpret distance-basis positions as fractions in [0, 1]. Invalid with
    ``basis='m'``.

unit : {'planar', 'meters'}, default None
    Distance-basis unit override. Omitted follows the CRS; invalid with
    ``basis='m'``."
    };
    (@tail) => {
        r"
Raises
------
GeometryTypeError
    If the geometry is not lineal.
InvalidGeometryError
    If the linework is empty, or M values are missing or non-monotonic.
GeometryError
    If locations are non-finite or out of order, or a distance-only option is
    used with ``basis='m'``.

See Also
--------
line_interpolate : Point at a location along the line.
line_locate : Project a geometry onto the line.
interpolate_m : Assign M ordinates along arc length.

Examples
--------
>>> import gometry as gm
>>> line = gm.LineString([(0, 0), (10, 0)])
>>> line.line_substring(2, 6).to_wkt()
'LINESTRING (2 0, 6 0)'"
    };
}

macro_rules! doc_interpolate_m {
    (scalar) => {
        concat!(doc_interpolate_m!(@pre), r"
start_m, end_m : float
    The measure range (finite, ``end_m >= start_m``).
", doc_interpolate_m!(@post), r"

Returns
-------
Geometry
    The geometry with interpolated M values (same kind as the input).

", doc_interpolate_m!(@tail))
    };
    (array) => {
        concat!(doc_interpolate_m!(@pre), r"
start_m, end_m : float or sequence of float
    The measure range (finite, ``end_m >= start_m``) — a scalar applies to
    every geometry, or pass one value per geometry.
", doc_interpolate_m!(@post), r"

Returns
-------
GeometryArray
    One result per row (kinds preserved).

", doc_interpolate_m!(@tail))
    };
    (@pre) => {
        r"Interpolate an M ordinate along the line's arc length (CRS-aware). M runs
from ``start_m`` at the start to ``end_m`` at the end, continuously across
multipart linework (the PostGIS ``ST_AddMeasure`` shape). The stationing
follows the CRS like length — geodesic on a geographic CRS, planar
otherwise (coordinates are never moved). Z is preserved; existing M requires
``overwrite=True``.

Parameters
----------"
    };
    (@post) => {
        r"overwrite : bool, default False
    Replace existing M ordinates instead of raising.

unit : {'planar', 'meters'}, default None
    Omitted follows the CRS: geodesic meters on a geographic CRS, native units on a projected one, coordinate units without a CRS. ``planar``
    forces raw coordinate units (degrees-as-Cartesian on a geographic CRS
    — only for deliberate coordinate-space math); ``meters`` forces the
    CRS metric and raises without a CRS."
    };
    (@tail) => {
        r"
Raises
------
GeometryTypeError
    If the geometry is not lineal.
InvalidGeometryError
    If the linework is empty, or carries M without ``overwrite``.
GeometryError
    If the measure range is invalid.

See Also
--------
line_interpolate : Point at a distance or M location along the line.
line_substring : Extract a contiguous portion of the line.
line_locate : Project a geometry onto the line.

Examples
--------
>>> import gometry as gm
>>> line = gm.LineString([(0, 0), (10, 0)])
>>> line.interpolate_m(0.0, 100.0).to_wkt()
'LINESTRING M (0 0 0, 10 0 100)'"
    };
}

macro_rules! doc_line_locate {
    (scalar) => {
        concat!(doc_line_locate!(@body), r"

Returns
-------
float or numpy.ndarray
    One location, or a column when ``geom`` is an array.

", doc_line_locate!(@tail))
    };
    (array) => {
        concat!(doc_line_locate!(@body), r"

Returns
-------
numpy.ndarray
    One location per line row.

", doc_line_locate!(@tail))
    };
    (@body) => {
        r"Locate the position on linework nearest ``geom``.

Parameters
----------
geom : Geometry or GeometryArray
    A geometry to project, or one geometry per line row.

basis : {'distance', 'm'}, default 'distance'
    Return a CRS-aware distance, or the line's monotonic M ordinate.

normalized : bool, default False
    Return a distance-basis fraction in [0, 1]. Invalid with ``basis='m'``.

unit : {'planar', 'meters'}, default None
    Distance-basis unit override. Omitted follows the CRS; invalid with
    ``basis='m'``."
    };
    (@tail) => {
        r"
Raises
------
CRSError
    If the CRS cannot provide an unambiguous distance metric.
CRSMismatchError
    If operands' CRS or coordinate-epoch metadata differ.
GeometryTypeError
    If the geometry is not lineal.
InvalidGeometryError
    If linework is empty, or M values are missing or non-monotonic.
GeometryError
    If a distance-only option is used with ``basis='m'``.

See Also
--------
line_interpolate : Point at a location along the line (inverse of locate).
line_substring : Extract a contiguous portion of the line.
interpolate_m : Assign M ordinates along arc length.

Examples
--------
>>> import gometry as gm
>>> line = gm.LineString([(0, 0), (10, 0)])
>>> line.line_locate(gm.Point(4, 3))
4.0"
    };
}

#[pymethods]
impl PyGeometry {
    #[doc = doc_line_interpolate!(scalar)]
    #[pyo3(
        signature = (at = None, /, *, count = None, basis = LineReferenceBasis::Distance, normalized = false, unit = None),
        text_signature = "($self, at=None, /, *, count=None, basis='distance', normalized=False, unit=None)"
    )]
    pub fn line_interpolate(
        &self,
        py: Python<'_>,
        at: Option<&Bound<'_, PyAny>>,
        count: Option<&Bound<'_, PyAny>>,
        basis: LineReferenceBasis,
        normalized: bool,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        if count.is_some() || at.is_none_or(|value| value.extract::<f64>().is_err()) {
            if basis != LineReferenceBasis::Distance {
                return Err(GeometryError::new_err(
                    "count and multiple at values require basis='distance'",
                ));
            }
            let plan = parse_interpolate_plan(count, at, normalized)?;
            return crate::dispatch::line_interpolate_points_scalar(self, &plan, unit)
                .and_then(|value| value.into_pyobject(py).map(|value| value.unbind().into()));
        }
        let at = at.expect("checked above");
        if basis == LineReferenceBasis::M {
            reject_m_basis_distance_options(normalized, unit)?;
            let m = F64Param::parse_raw(at, "at", unary_len!(scalar))?;
            let value = unary_spine_shapes!(
                scalar,
                py,
                self,
                crate::dispatch::Operation::LineInterpolate,
                None,
                default,
                move |data, ctx| crate::dispatch::kernels::unary_line_interpolate_point_m(
                    data, ctx, &m
                )
            )?;
            return value.into_pyobject(py).map(Bound::unbind);
        }
        let op = crate::dispatch::Operation::LineInterpolate;
        let distance = F64Param::parse_raw(at, "at", unary_len!(scalar))?;
        let model = op
            .resolver_with_line_unit(unit, normalized)
            .resolve_ctx(
                &self.frame,
                op.name(),
                &mut crate::dispatch::MetricScratch::default(),
            )?
            .require_model(op.name())?
            .clone();
        let value = unary_spine_shapes!(scalar, py, self, op, None, default, move |data, ctx| {
            crate::dispatch::kernels::unary_line_interpolate_point(
                data, ctx, &model, &distance, normalized,
            )
        })?;
        value.into_pyobject(py).map(Bound::unbind)
    }

    #[doc = doc_line_substring!(scalar)]
    #[pyo3(
        signature = (start, end, *, basis = LineReferenceBasis::Distance, normalized = false, unit = None),
        text_signature = "($self, start, end, *, basis='distance', normalized=False, unit=None)"
    )]
    pub fn line_substring(
        &self,
        py: Python<'_>,
        start: &Bound<'_, PyAny>,
        end: &Bound<'_, PyAny>,
        basis: LineReferenceBasis,
        normalized: bool,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Typed> {
        if basis == LineReferenceBasis::M {
            reject_m_basis_distance_options(normalized, unit)?;
            let start = F64Param::parse_raw(start, "start", unary_len!(scalar))?;
            let end = F64Param::parse_raw(end, "end", unary_len!(scalar))?;
            return unary_spine_shapes!(
                scalar,
                py,
                self,
                crate::dispatch::Operation::LineSubstring,
                None,
                default,
                move |data, ctx| crate::dispatch::kernels::unary_line_substring_m(
                    data, ctx, &start, &end
                )
            );
        }
        let op = crate::dispatch::Operation::LineSubstring;
        let start_distance = F64Param::parse_raw(start, "start", unary_len!(scalar))?;
        let end_distance = F64Param::parse_raw(end, "end", unary_len!(scalar))?;
        let model = op
            .resolver_with_line_unit(unit, normalized)
            .resolve_ctx(
                &self.frame,
                op.name(),
                &mut crate::dispatch::MetricScratch::default(),
            )?
            .require_model(op.name())?
            .clone();
        unary_spine_shapes!(scalar, py, self, op, None, default, move |data, ctx| {
            crate::dispatch::kernels::unary_line_substring(
                data,
                ctx,
                &model,
                &start_distance,
                &end_distance,
                normalized,
            )
        })
    }

    #[doc = doc_interpolate_m!(scalar)]
    #[pyo3(signature = (start_m, end_m, *, overwrite = false, unit = None))]
    pub fn interpolate_m(
        &self,
        py: Python<'_>,
        start_m: &Bound<'_, PyAny>,
        end_m: &Bound<'_, PyAny>,
        overwrite: bool,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Typed> {
        let start_m = F64Param::parse_raw(start_m, "start_m", unary_len!(scalar))?;
        let end_m = F64Param::parse_raw(end_m, "end_m", unary_len!(scalar))?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::InterpolateM,
            unit,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_interpolate_m(
                data, ctx, &start_m, &end_m, overwrite,
            )
        )
    }

    #[doc = doc_line_locate!(scalar)]
    #[pyo3(
        signature = (geom, *, basis = LineReferenceBasis::Distance, normalized = false, unit = None),
        text_signature = "($self, geom, *, basis='distance', normalized=False, unit=None)"
    )]
    pub fn line_locate(
        &self,
        py: Python<'_>,
        geom: &Bound<'_, PyAny>,
        basis: LineReferenceBasis,
        normalized: bool,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        if basis == LineReferenceBasis::M {
            reject_m_basis_distance_options(normalized, unit)?;
            return crate::dispatch::line_locate_point_m_input(py, GeometryInput::One(self), geom);
        }
        crate::dispatch::line_locate_point_input(
            py,
            GeometryInput::One(self),
            geom,
            normalized,
            unit,
        )
    }
}

#[pymethods]
impl PyGeometryArray {
    #[doc = doc_line_interpolate!(array)]
    #[pyo3(
        signature = (at = None, /, *, count = None, basis = LineReferenceBasis::Distance, normalized = false, unit = None),
        text_signature = "($self, at=None, /, *, count=None, basis='distance', normalized=False, unit=None)"
    )]
    pub fn line_interpolate(
        &self,
        py: Python<'_>,
        at: Option<&Bound<'_, PyAny>>,
        count: Option<&Bound<'_, PyAny>>,
        basis: LineReferenceBasis,
        normalized: bool,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        if count.is_some() {
            if basis != LineReferenceBasis::Distance || at.is_some() {
                return Err(GeometryError::new_err(
                    "count requires basis='distance' and no at value",
                ));
            }
            return crate::array::line_interpolate_points_rows(
                py, self, count, None, normalized, unit,
            )?
            .into_pyobject(py)
            .map(|value| value.unbind().into());
        }
        let at =
            at.ok_or_else(|| GeometryError::new_err("line_interpolate requires at or count"))?;
        if basis == LineReferenceBasis::M {
            reject_m_basis_distance_options(normalized, unit)?;
            let m = F64Param::parse_raw(at, "at", unary_len!(array, self))?;
            let value = unary_spine_shapes!(
                array,
                py,
                self,
                crate::dispatch::Operation::LineInterpolate,
                None,
                default,
                move |data, ctx| crate::dispatch::kernels::unary_line_interpolate_point_m(
                    data, ctx, &m
                )
            )?;
            return value.into_pyobject(py).map(|value| value.unbind().into());
        }
        let op = crate::dispatch::Operation::LineInterpolate;
        let distance = F64Param::parse_raw(at, "at", unary_len!(array, self))?;
        let model = op
            .resolver_with_line_unit(unit, normalized)
            .resolve_ctx(
                &self.frame,
                op.name(),
                &mut crate::dispatch::MetricScratch::default(),
            )?
            .require_model(op.name())?
            .clone();
        let value = unary_spine_shapes_extras!(
            array,
            py,
            self,
            op,
            None,
            crate::dispatch::PackedUnary::LineInterpolatePoint {
                distance: distance.clone(),
                normalized,
                metric: model.clone(),
            },
            move |data, ctx| crate::dispatch::kernels::unary_line_interpolate_point(
                data, ctx, &model, &distance, normalized,
            )
        )?;
        value.into_pyobject(py).map(|value| value.unbind().into())
    }

    #[doc = doc_line_substring!(array)]
    #[pyo3(
        signature = (start, end, *, basis = LineReferenceBasis::Distance, normalized = false, unit = None),
        text_signature = "($self, start, end, *, basis='distance', normalized=False, unit=None)"
    )]
    pub fn line_substring(
        &self,
        py: Python<'_>,
        start: &Bound<'_, PyAny>,
        end: &Bound<'_, PyAny>,
        basis: LineReferenceBasis,
        normalized: bool,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        if basis == LineReferenceBasis::M {
            reject_m_basis_distance_options(normalized, unit)?;
            let start = F64Param::parse_raw(start, "start", unary_len!(array, self))?;
            let end = F64Param::parse_raw(end, "end", unary_len!(array, self))?;
            return unary_spine_shapes!(
                array,
                py,
                self,
                crate::dispatch::Operation::LineSubstring,
                None,
                default,
                move |data, ctx| crate::dispatch::kernels::unary_line_substring_m(
                    data, ctx, &start, &end
                )
            );
        }
        let op = crate::dispatch::Operation::LineSubstring;
        let start_distance = F64Param::parse_raw(start, "start", unary_len!(array, self))?;
        let end_distance = F64Param::parse_raw(end, "end", unary_len!(array, self))?;
        let model = op
            .resolver_with_line_unit(unit, normalized)
            .resolve_ctx(
                &self.frame,
                op.name(),
                &mut crate::dispatch::MetricScratch::default(),
            )?
            .require_model(op.name())?
            .clone();
        unary_spine_shapes_extras!(
            array,
            py,
            self,
            op,
            None,
            crate::dispatch::PackedUnary::LineSubstring {
                start_distance: start_distance.clone(),
                end_distance: end_distance.clone(),
                normalized,
                metric: model.clone(),
            },
            move |data, ctx| crate::dispatch::kernels::unary_line_substring(
                data,
                ctx,
                &model,
                &start_distance,
                &end_distance,
                normalized,
            )
        )
    }

    #[doc = doc_interpolate_m!(array)]
    #[pyo3(signature = (start_m, end_m, *, overwrite = false, unit = None))]
    pub fn interpolate_m(
        &self,
        py: Python<'_>,
        start_m: &Bound<'_, PyAny>,
        end_m: &Bound<'_, PyAny>,
        overwrite: bool,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Self> {
        let start_m = F64Param::parse_raw(start_m, "start_m", unary_len!(array, self))?;
        let end_m = F64Param::parse_raw(end_m, "end_m", unary_len!(array, self))?;
        unary_spine_shapes!(
            array,
            py,
            self,
            crate::dispatch::Operation::InterpolateM,
            unit,
            default,
            move |data, ctx| crate::dispatch::kernels::unary_interpolate_m(
                data, ctx, &start_m, &end_m, overwrite,
            )
        )
    }

    #[doc = doc_line_locate!(array)]
    #[pyo3(
        signature = (geom, *, basis = LineReferenceBasis::Distance, normalized = false, unit = None),
        text_signature = "($self, geom, *, basis='distance', normalized=False, unit=None)"
    )]
    pub fn line_locate(
        &self,
        py: Python<'_>,
        geom: &Bound<'_, PyAny>,
        basis: LineReferenceBasis,
        normalized: bool,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        if basis == LineReferenceBasis::M {
            reject_m_basis_distance_options(normalized, unit)?;
            return crate::dispatch::line_locate_point_m_input(py, GeometryInput::Many(self), geom);
        }
        crate::dispatch::line_locate_point_input(
            py,
            GeometryInput::Many(self),
            geom,
            normalized,
            unit,
        )
    }
}
