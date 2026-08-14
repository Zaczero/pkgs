use pyo3::exceptions::PyAttributeError;

use crate::py::classes::leaf_methods::{
    EmptyKind, PyLineString, PyPoint, PyPolygon, PyRef, PyResult, Python, Shape, Typed, pymethods,
};
use crate::py::errors::geometry_type_err;
use crate::{Bound, DistanceUnit, NavigationPath, Py, PyAny};

enum PointAxis {
    X,
    Y,
    Z,
    M,
}

impl PyPoint {
    fn point_ordinate(slf: &PyRef<'_, Self>, axis: PointAxis) -> PyResult<f64> {
        match slf.as_super().shape.shape() {
            Shape::Point(point) => match axis {
                PointAxis::X => Ok(point.x),
                PointAxis::Y => Ok(point.y),
                PointAxis::Z => point
                    .z()
                    .ok_or_else(|| geometry_type_err("Point geometry has no Z coordinate")),
                PointAxis::M => point
                    .m()
                    .ok_or_else(|| geometry_type_err("Point geometry has no M coordinate")),
            },
            Shape::Empty(EmptyKind::Point, _) => match axis {
                PointAxis::X => Err(PyAttributeError::new_err("empty point has no x coordinate")),
                PointAxis::Y => Err(PyAttributeError::new_err("empty point has no y coordinate")),
                PointAxis::Z => Err(PyAttributeError::new_err("empty point has no z coordinate")),
                PointAxis::M => Err(PyAttributeError::new_err("empty point has no m coordinate")),
            },
            _ => unreachable!("Point wraps a Point shape"),
        }
    }
}

#[pymethods]
impl PyPoint {
    /// Walk from this point along a geodesic or rhumb path.
    ///
    /// Parameters
    /// ----------
    /// bearing : float or sequence of float
    ///     Initial azimuth in degrees clockwise from north.
    /// distance : float or sequence of float
    ///     Distance to travel in CRS-natural units, unless `unit` is set.
    /// path : {'geodesic', 'rhumb'}, default 'geodesic'
    ///     Route model. Rhumb paths require a geographic CRS and use meters.
    /// unit : {'planar', 'meters'}, optional
    ///     Force coordinate units or CRS metric units.
    ///
    /// Returns
    /// -------
    /// Point or GeometryArray[Point]
    ///     The destination point, or one destination per bearing or distance.
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If `bearing` or `distance` is invalid.
    /// CRSError
    ///     If the selected route requires an unavailable CRS metric.
    /// InvalidGeometryError
    ///     If a coordinate is outside the longitude/latitude domain.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> point = gm.Point(0, 0, crs=4326)
    /// >>> point.destination(90, 1000).to_wkt(precision=5)
    /// 'POINT (0.00898 0)'
    #[pyo3(
        signature = (bearing, distance, *, path = NavigationPath::Geodesic, unit = None),
        text_signature = "($self, bearing, distance, *, path='geodesic', unit=None)"
    )]
    pub(crate) fn destination(
        slf: PyRef<'_, Self>,
        py: Python<'_>,
        bearing: &Bound<'_, PyAny>,
        distance: &Bound<'_, PyAny>,
        path: NavigationPath,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>> {
        match path {
            NavigationPath::Geodesic => crate::measures::destination_point_receiver(
                py,
                slf.as_super(),
                bearing,
                distance,
                unit,
            ),
            NavigationPath::Rhumb => {
                crate::measures::reject_rhumb_unit(unit, "destination")?;
                crate::measures::rhumb_destination_point_receiver(
                    py,
                    slf.as_super(),
                    bearing,
                    distance,
                )
            },
        }
    }

    /// ``case Point(x, y)`` destructures the ordinates. ``POINT EMPTY`` does
    /// not match because its X/Y attributes are absent.
    #[classattr]
    const fn __match_args__() -> (&'static str, &'static str) {
        ("x", "y")
    }

    /// X coordinate of the point.
    ///
    /// Raises
    /// ------
    /// AttributeError
    ///     If the point is empty (``POINT EMPTY``).
    #[getter]
    pub(crate) fn x(slf: PyRef<'_, Self>) -> PyResult<f64> {
        Self::point_ordinate(&slf, PointAxis::X)
    }

    /// Y coordinate of the point.
    ///
    /// Raises
    /// ------
    /// AttributeError
    ///     If the point is empty (``POINT EMPTY``).
    #[getter]
    pub(crate) fn y(slf: PyRef<'_, Self>) -> PyResult<f64> {
        Self::point_ordinate(&slf, PointAxis::Y)
    }

    /// Z (elevation) ordinate.
    ///
    /// Raises
    /// ------
    /// GeometryTypeError
    ///     If the point has no Z ordinate.
    /// AttributeError
    ///     If the point is empty (``POINT EMPTY``).
    #[getter]
    pub(crate) fn z(slf: PyRef<'_, Self>) -> PyResult<f64> {
        Self::point_ordinate(&slf, PointAxis::Z)
    }

    /// M (measure) ordinate.
    ///
    /// Raises
    /// ------
    /// GeometryTypeError
    ///     If the point has no M ordinate.
    /// AttributeError
    ///     If the point is empty (``POINT EMPTY``).
    #[getter]
    pub(crate) fn m(slf: PyRef<'_, Self>) -> PyResult<f64> {
        Self::point_ordinate(&slf, PointAxis::M)
    }
}

#[pymethods]
impl PyLineString {
    /// ``case LineString(coords)`` destructures the coordinate view, so a
    /// `LineString` nested inside a `Polygon(exterior, _)` match can recurse.
    #[classattr]
    const fn __match_args__() -> (&'static str,) {
        ("coords",)
    }
}

#[pymethods]
impl PyPolygon {
    /// ``case Polygon(shell, holes)`` destructures the rings.
    #[classattr]
    const fn __match_args__() -> (&'static str, &'static str) {
        ("exterior", "interiors")
    }

    /// Exterior ring as a closed ``LineString``.
    #[getter]
    pub(crate) fn exterior(slf: PyRef<'_, Self>) -> Typed {
        let base = slf.as_super();
        let ring = base.shape.exterior().expect("Polygon has an exterior ring");
        base.typed_shape(ring)
    }

    /// Interior rings (holes), each a closed ``LineString``.
    #[getter]
    pub(crate) fn interiors(slf: PyRef<'_, Self>) -> Vec<Typed> {
        let base = slf.as_super();
        base.shape
            .interiors()
            .into_iter()
            .map(|ring| base.typed_shape(ring))
            .collect()
    }
}
