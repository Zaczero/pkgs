use pyo3::exceptions::PyAttributeError;

use crate::py::classes::leaf_methods::{
    EmptyKind, PyLineString, PyPoint, PyPolygon, PyRef, PyResult, Shape, Typed, pymethods,
};
use crate::py::errors::geometry_type_err;

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
