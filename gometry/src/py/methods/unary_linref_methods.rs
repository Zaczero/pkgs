use pyo3::prelude::*;

use crate::dispatch::{MetricOp, kernels};
use crate::py::errors::GeometryError;
use crate::py::support::LineReferenceBasis;
use crate::{
    DistanceUnit, F64Param, GeometryInput, NavigationPath, PyGeometry, PyGeometryArray, Typed,
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

method_doc! {
    doc_line_interpolate;
    pre: r"Interpolate point locations along linework. Empty linework yields
``POINT EMPTY`` at every location.

Parameters
----------
at : float or sequence of float, optional
    One location or many explicit distance-basis locations. Under
    ``basis='m'``, pass one stored M value (or one value per array row).

";
    scalar: (r"
count : int, optional
    Number of evenly spaced distance-basis samples (``>= 1``). Mutually
    exclusive with ``at`` and unavailable with ``basis='m'``.
", r"

Returns
-------
Point or GeometryArray[Point]
    One point for scalar ``at``; a point column for many ``at`` values or
    ``count`` samples.

");
    array: (r"
count : int or iterable of int, optional
    Number of evenly spaced distance-basis samples per row (``>= 1``). A scalar
    broadcasts; otherwise pass one count per row. Mutually exclusive with
    ``at`` and unavailable with ``basis='m'``.
", r"

Returns
-------
GeometryArray[Point] or Groups[GeometryArray[Point]]
    One point per row for scalar or row-aligned ``at``; one point group per
    row for ``count`` samples.

");
    post: concat!(r"
basis : {'distance', 'm'}, default 'distance'
    Use CRS-aware distance, or the line's monotonic M ordinate. For ``basis='m'``,
    synthesized coordinates follow the CRS metric: geodesic on a geographic CRS,
    planar otherwise.

normalized : bool, default False
    Interpret distance-basis ``at`` values as fractions in [0, 1]. Invalid
    with ``basis='m'``.

", unit_parameter_doc!(), r"    Available only with ``basis='distance'``; invalid with
    ``basis='m'``.");
    tail: r"
Raises
------
GeometryTypeError
    If the geometry is not lineal.
InvalidGeometryError
    If M values are missing or non-monotonic; an array row degrades to an
    empty result instead.
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
'POINT (4 0)'";
}

method_doc! {
    doc_line_substring;
    body: concat!(r"Return the portion of linework from ``start`` through ``end``. Empty
linework yields ``LINESTRING EMPTY``.

Parameters
----------
start, end : float or sequence of float
    Ordered locations on the selected basis. Distance values follow the CRS;
    M values are stored route measures. A scalar applies to every array row.

basis : {'distance', 'm'}, default 'distance'
    Use CRS-aware distance, or the line's monotonic M ordinate. For ``basis='m'``,
    synthesized coordinates follow the CRS metric: geodesic on a geographic CRS,
    planar otherwise.

normalized : bool, default False
    Interpret distance-basis positions as fractions in [0, 1]. Invalid with
    ``basis='m'``.
", unit_parameter_doc!(), r"    Available only with ``basis='distance'``; invalid with
    ``basis='m'``.");
    scalar: r"

Returns
-------
LineString or Point
    The substring (a ``Point`` when ``start == end``).

";
    array: r"

Returns
-------
GeometryArray
    One substring per row.

";
    tail: r"
Raises
------
GeometryTypeError
    If the geometry is not lineal.
InvalidGeometryError
    If M values are missing or non-monotonic; an array row degrades to an
    empty result instead.
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
'LINESTRING (2 0, 6 0)'";
}

method_doc! {
    doc_interpolate_m;
    pre: r"Interpolate an M ordinate along the line's arc length (CRS-aware). M runs
from ``start_m`` at the start to ``end_m`` at the end, continuously across
multipart linework (the PostGIS ``ST_AddMeasure`` shape). The stationing
follows the CRS like length — geodesic on a geographic CRS, planar
otherwise (coordinates are never moved). Z is preserved; existing M requires
``overwrite=True``. Empty linework returns the same empty with an M axis.

Parameters
----------";
    scalar: (r"
start_m, end_m : float
    The measure range (finite, ``end_m >= start_m``).
", r"

Returns
-------
Geometry
    The geometry with interpolated M values (same kind as the input).

");
    array: (r"
start_m, end_m : float or sequence of float
    The measure range (finite, ``end_m >= start_m``) — a scalar applies to
    every geometry, or pass one value per geometry.
", r"

Returns
-------
GeometryArray
    One result per row (kinds preserved).

");
    post: concat!(r"overwrite : bool, default False
    Replace existing M ordinates instead of raising.

", unit_parameter_doc!());
    tail: r"
Raises
------
GeometryTypeError
    If the geometry is not lineal.
InvalidGeometryError
    If the linework carries M without ``overwrite``.
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
'LINESTRING M (0 0 0, 10 0 100)'";
}

method_doc! {
    doc_line_locate;
    body: concat!(r"Locate the position on linework nearest ``geom``. Empty linework, or
an empty query point, locates to ``nan``.

Parameters
----------
geom : Geometry or GeometryArray
    A geometry to project, or one geometry per line row.

basis : {'distance', 'm'}, default 'distance'
    Return a CRS-aware distance, or the line's monotonic M ordinate. For
    ``basis='m'``, nearest-point projection follows the CRS metric: geodesic on a
    geographic CRS, planar otherwise.

normalized : bool, default False
    Return a distance-basis fraction in [0, 1]. Invalid with ``basis='m'``.

", unit_parameter_doc!(), r"    Available only with ``basis='distance'``; invalid with
    ``basis='m'``.");
    scalar: r"

Returns
-------
float or numpy.ndarray
    One location, or a column when ``geom`` is an array.

";
    array: r"

Returns
-------
numpy.ndarray
    One location per line row.

";
    tail: r"
Raises
------
CRSError
    If the CRS cannot provide an unambiguous distance metric.
CRSMismatchError
    If operands' CRS or coordinate-epoch metadata differ.
GeometryTypeError
    If the geometry is not lineal.
InvalidGeometryError
    If M values are missing or non-monotonic; an array row degrades to
    ``nan`` instead.
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
4.0";
}

#[pymethods]
impl PyGeometry {
    #[doc = doc_line_interpolate!(scalar)]
    #[pyo3(
        signature = (at = None, *, count = None, basis = LineReferenceBasis::Distance, normalized = false, unit = None),
        text_signature = "($self, at=None, *, count=None, basis='distance', normalized=False, unit=None)"
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
        let at_scalar = at
            .map(|value| crate::f64_scalar_named("at", value))
            .transpose()?
            .flatten();
        if count.is_some() || at_scalar.is_none() {
            if basis != LineReferenceBasis::Distance {
                return Err(GeometryError::new_err(
                    "count and multiple at values require basis='distance'",
                ));
            }
            let plan = parse_interpolate_plan(count, at, normalized)?;
            return crate::dispatch::line_interpolate_points_scalar(self, &plan, unit)
                .and_then(|value| value.into_pyobject(py).map(|value| value.unbind().into()));
        }
        let at = at_scalar.expect("checked above");
        if basis == LineReferenceBasis::M {
            reject_m_basis_distance_options(normalized, unit)?;
            let m = F64Param::Scalar(at);
            m.ensure_finite("at")?;
            let value = unary_spine_shapes!(
                scalar,
                py,
                self,
                crate::dispatch::Operation::Metric(MetricOp::LineInterpolate),
                None,
                default,
                move |data, ctx| kernels::unary_line_interpolate_point_m(data, ctx, &m)
            )?;
            return value.into_pyobject(py).map(Bound::unbind);
        }
        let op = crate::dispatch::Operation::Metric(MetricOp::LineInterpolate);
        let distance = F64Param::Scalar(at);
        distance.ensure_finite("at")?;
        let resolver = op.resolver_with_line_unit(unit, normalized);
        let value = crate::dispatch::unary_scalar_linref_shape(
            py,
            self,
            op,
            resolver,
            move |data, ctx| {
                kernels::unary_line_interpolate_point(data, ctx, &distance, normalized)
            },
        )?;
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
            start.ensure_finite("start")?;
            end.ensure_finite("end")?;
            return unary_spine_shapes!(
                scalar,
                py,
                self,
                crate::dispatch::Operation::Metric(MetricOp::LineSubstring),
                None,
                default,
                move |data, ctx| kernels::unary_line_substring_m(data, ctx, &start, &end)
            );
        }
        let op = crate::dispatch::Operation::Metric(MetricOp::LineSubstring);
        let start_distance = F64Param::parse_raw(start, "start", unary_len!(scalar))?;
        let end_distance = F64Param::parse_raw(end, "end", unary_len!(scalar))?;
        start_distance.ensure_finite("start")?;
        end_distance.ensure_finite("end")?;
        let resolver = op.resolver_with_line_unit(unit, normalized);
        crate::dispatch::unary_scalar_linref_shape(py, self, op, resolver, move |data, ctx| {
            kernels::unary_line_substring(data, ctx, &start_distance, &end_distance, normalized)
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
        start_m.ensure_finite("start_m")?;
        end_m.ensure_finite("end_m")?;
        unary_spine_shapes!(
            scalar,
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::InterpolateM),
            unit,
            default,
            move |data, ctx| kernels::unary_interpolate_m(data, ctx, &start_m, &end_m, overwrite,)
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
        }
        crate::dispatch::line_locate_input(
            py,
            GeometryInput::One(self),
            geom,
            basis,
            normalized,
            unit,
        )
    }
}

#[pymethods]
impl PyGeometryArray {
    /// Walk each point along a geodesic or rhumb path. An empty point stays
    /// ``POINT EMPTY``.
    ///
    /// Parameters
    /// ----------
    /// bearing : float or sequence of float
    ///     Initial azimuth in degrees clockwise from north.
    /// distance : float or sequence of float
    ///     Finite, non-negative distance to travel in CRS-natural units, unless
    ///     `unit` is set.
    /// path : {'geodesic', 'rhumb'}, default 'geodesic'
    ///     Route model. Rhumb paths require a geographic CRS and use meters.
    #[doc = unit_parameter_doc!()]
    ///     ``path='rhumb'`` uses meters and does not accept a unit override.
    ///
    /// Returns
    /// -------
    /// GeometryArray[Point]
    ///     One destination point per array row.
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If `bearing` or `distance` is invalid.
    /// CRSError
    ///     If the selected route requires an unavailable CRS metric.
    /// GeometryTypeError
    ///     If an input row is not a point.
    /// InvalidGeometryError
    ///     If a coordinate is outside the longitude/latitude domain.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> points = gm.GeometryArray([
    /// ...     gm.Point(0, 0, crs=4326), gm.Point(1, 1, crs=4326)
    /// ... ])
    /// >>> points.destination(90, 1000).to_wkt(precision=5)[0]
    /// 'POINT (0.00898 0)'
    #[pyo3(
        signature = (bearing, distance, *, path = NavigationPath::Geodesic, unit = None),
        text_signature = "($self, bearing, distance, *, path='geodesic', unit=None)"
    )]
    pub fn destination(
        &self,
        py: Python<'_>,
        bearing: &Bound<'_, PyAny>,
        distance: &Bound<'_, PyAny>,
        path: NavigationPath,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        let points = Py::new(py, self.clone())?;
        crate::measures::destination(py, points.bind(py), bearing, distance, path, unit)
    }

    #[doc = doc_line_interpolate!(array)]
    #[pyo3(
        signature = (at = None, *, count = None, basis = LineReferenceBasis::Distance, normalized = false, unit = None),
        text_signature = "($self, at=None, *, count=None, basis='distance', normalized=False, unit=None)"
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
            m.ensure_finite("at")?;
            let value = unary_spine_shapes!(
                array,
                py,
                self,
                crate::dispatch::Operation::Metric(MetricOp::LineInterpolate),
                None,
                default,
                move |data, ctx| kernels::unary_line_interpolate_point_m(data, ctx, &m)
            )?;
            return value.into_pyobject(py).map(|value| value.unbind().into());
        }
        let op = crate::dispatch::Operation::Metric(MetricOp::LineInterpolate);
        let distance = F64Param::parse_raw(at, "at", unary_len!(array, self))?;
        distance.ensure_finite("at")?;
        let resolver = op.resolver_with_line_unit(unit, normalized);
        let model = resolver
            .resolve_ctx(&self.frame, &mut crate::dispatch::MetricScratch::default())?
            .require_model(MetricOp::LineInterpolate)?
            .clone();
        let packed = crate::dispatch::PackedLinearReference::InterpolatePoint {
            distance: distance.clone(),
            normalized,
            metric: model,
        };
        let value =
            crate::dispatch::unary_array_linref_shapes(py, self, &packed, move |data, ctx| {
                kernels::unary_line_interpolate_point(data, ctx, &distance, normalized)
            })?;
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
            start.ensure_finite("start")?;
            end.ensure_finite("end")?;
            return unary_spine_shapes!(
                array,
                py,
                self,
                crate::dispatch::Operation::Metric(MetricOp::LineSubstring),
                None,
                default,
                move |data, ctx| kernels::unary_line_substring_m(data, ctx, &start, &end)
            );
        }
        let op = crate::dispatch::Operation::Metric(MetricOp::LineSubstring);
        let start_distance = F64Param::parse_raw(start, "start", unary_len!(array, self))?;
        let end_distance = F64Param::parse_raw(end, "end", unary_len!(array, self))?;
        start_distance.ensure_finite("start")?;
        end_distance.ensure_finite("end")?;
        let resolver = op.resolver_with_line_unit(unit, normalized);
        let model = resolver
            .resolve_ctx(&self.frame, &mut crate::dispatch::MetricScratch::default())?
            .require_model(MetricOp::LineSubstring)?
            .clone();
        let packed = crate::dispatch::PackedLinearReference::Substring {
            start_distance: start_distance.clone(),
            end_distance: end_distance.clone(),
            normalized,
            metric: model,
        };
        crate::dispatch::unary_array_linref_shapes(py, self, &packed, move |data, ctx| {
            kernels::unary_line_substring(data, ctx, &start_distance, &end_distance, normalized)
        })
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
        start_m.ensure_finite("start_m")?;
        end_m.ensure_finite("end_m")?;
        unary_spine_shapes!(
            array,
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::InterpolateM),
            unit,
            default,
            move |data, ctx| kernels::unary_interpolate_m(data, ctx, &start_m, &end_m, overwrite,)
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
        }
        crate::dispatch::line_locate_input(
            py,
            GeometryInput::Many(self),
            geom,
            basis,
            normalized,
            unit,
        )
    }
}
