//! Callable leaf constructors (`Point(...)`, `LineString(...)`, ...).
//!
//! Each `#[new]` builds the base [`PyGeometry`] via the shared kernels in
//! [`crate::py::functions::constructors`], then attaches the typed leaf marker through
//! [`PyClassInitializer`].

use std::sync::Arc;

use pyo3::prelude::{PyClassInitializer, PyResult};
use pyo3::types::PyDict;
use pyo3::{Bound, PyAny};

use super::*;
use crate::boundary::metadata::FrameEdit;
use crate::geometry::{EmptyKind, MOrdinate, Point, Shape, ZOrdinate};
use crate::py::errors::InvalidGeometryError;
use crate::py::functions::constructors::{
    build_geometry_collection, build_line_string, build_multi_line_string, build_multi_point,
    build_multi_polygon, build_point, build_polygon,
};
use crate::py::replace::{
    ReplacePresence, reject_unknown_kwargs, replace_crs, replace_epoch, replace_optional_f64,
    replace_presence,
};

macro_rules! leaf_initializer {
    ($base:expr) => {
        PyClassInitializer::from($base).add_subclass(Self)
    };
}

#[pymethods]
impl PyPoint {
    /// Create a ``Point`` geometry — ``XY``, ``XYZ``, ``XYM``, or ``XYZM``.
    ///
    /// Pass ``z`` and/or ``m`` for higher-dimensional points; to build many
    /// points at once use ``points``.
    ///
    /// Parameters
    /// ----------
    /// x, y : float, optional
    ///     Finite coordinates (lon, lat for a geographic ``crs``). Omit both for
    ///     an empty point.
    ///
    /// z : float, optional
    ///     Z (elevation) ordinate, producing an ``XYZ`` or ``XYZM`` point.
    ///
    /// m : float, optional
    ///     M (measure) ordinate, producing an ``XYM`` or ``XYZM`` point.
    ///
    /// crs : str or int, optional
    ///     CRS as an EPSG code or authority/WKT. Declares; no transform.
    ///
    /// epoch : float, optional
    ///     Coordinate epoch (decimal year) for time-dependent frames.
    ///
    /// Returns
    /// -------
    /// Point
    ///     A point geometry — empty when ``x``/``y`` are omitted.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If any coordinate is not finite.
    /// CRSError
    ///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    /// GeometryError
    ///     If ``epoch`` is not a finite decimal year.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.Point(1, 2).to_wkt()
    /// 'POINT (1 2)'
    /// >>> gm.Point(1, 2, z=3).to_wkt()
    /// 'POINT Z (1 2 3)'
    /// >>> gm.Point(1, 2, m=9).to_wkt()
    /// 'POINT M (1 2 9)'
    /// >>> gm.Point(13.4, 52.5, crs=4326).crs
    /// CRS("EPSG:4326")
    /// >>> gm.Point().to_wkt()
    /// 'POINT EMPTY'
    #[new]
    #[pyo3(
        signature = (x = None, y = None, *, z = None, m = None, crs = None, epoch = None),
        text_signature = "(x=None, y=None, *, z=None, m=None, crs=None, epoch=None)"
    )]
    fn new(
        x: Option<&Bound<'_, PyAny>>,
        y: Option<&Bound<'_, PyAny>>,
        z: Option<&Bound<'_, PyAny>>,
        m: Option<&Bound<'_, PyAny>>,
        crs: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyClassInitializer<Self>> {
        Ok(leaf_initializer!(build_point(x, y, z, m, crs, epoch)?))
    }

    /// Return a copy with the given ordinates and metadata replaced.
    ///
    /// Supports ``copy.replace`` on Python 3.13+; omitted keyword arguments
    /// keep the current value. ``crs=None`` / ``epoch=None`` clear metadata;
    /// ``z=None`` / ``m=None`` drop those ordinates.
    ///
    /// Parameters
    /// ----------
    /// x, y : float, optional
    ///     Replace the X/Y coordinates.
    ///
    /// z, m : float or None, optional
    ///     Replace or clear the Z/M ordinates.
    ///
    /// crs : str or int or None, optional
    ///     Replace or clear the CRS label.
    ///
    /// epoch : float or None, optional
    ///     Replace or clear the coordinate epoch.
    ///
    /// Returns
    /// -------
    /// Point
    #[pyo3(signature = (*, **kwargs), text_signature = "($self, /, *, x=..., y=..., z=..., m=..., crs=..., epoch=...)")]
    fn __replace__(slf: PyRef<'_, Self>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Typed> {
        reject_unknown_kwargs(
            kwargs,
            &["x", "y", "z", "m", "crs", "epoch"],
            "Point.__replace__",
        )?;
        let base = slf.as_super();
        let x_edit = replace_presence(kwargs, "x")?;
        let y_edit = replace_presence(kwargs, "y")?;
        let z_edit = replace_optional_f64(kwargs, "z", "z")?;
        let m_edit = replace_optional_f64(kwargs, "m", "m")?;

        let shape = match base.shape.shape() {
            Shape::Empty(EmptyKind::Point, axes) => {
                // Stay empty unless X/Y materialize; preserve declared axes on
                // no-op and route Z/M through axis-aware empty retags.
                let mut empty = Shape::typed_empty(EmptyKind::Point, *axes);
                if let ReplacePresence::Set(value) = z_edit {
                    empty = empty.set_z(value, true)?;
                }
                if let ReplacePresence::Set(value) = m_edit {
                    empty = empty.set_m(value, true)?;
                }
                match (x_edit, y_edit) {
                    (ReplacePresence::Unset, ReplacePresence::Unset) => empty,
                    (ReplacePresence::Set(x), ReplacePresence::Set(y)) => {
                        // Materialize: require every ordinate declared by the
                        // resulting empty axes rather than silently flattening.
                        let axes = empty.axes();
                        let z = match z_edit {
                            ReplacePresence::Unset if axes.has_z() => {
                                return Err(InvalidGeometryError::new_err(
                                    "point materialization requires z for a Z-tagged empty",
                                ));
                            },
                            ReplacePresence::Unset => None,
                            ReplacePresence::Set(value) => value,
                        };
                        let m = match m_edit {
                            ReplacePresence::Unset if axes.has_m() => {
                                return Err(InvalidGeometryError::new_err(
                                    "point materialization requires m for an M-tagged empty",
                                ));
                            },
                            ReplacePresence::Unset => None,
                            ReplacePresence::Set(value) => value,
                        };
                        Shape::Point(Point::new_axes(x, y, ZOrdinate(z), MOrdinate(m))?)
                    },
                    _ => {
                        return Err(InvalidGeometryError::new_err(
                            "point requires both x and y, or neither",
                        ));
                    },
                }
            },
            Shape::Point(point) => {
                let x = match x_edit {
                    ReplacePresence::Unset => point.x,
                    ReplacePresence::Set(value) => value,
                };
                let y = match y_edit {
                    ReplacePresence::Unset => point.y,
                    ReplacePresence::Set(value) => value,
                };
                let z = match z_edit {
                    ReplacePresence::Unset => point.z(),
                    ReplacePresence::Set(value) => value,
                };
                let m = match m_edit {
                    ReplacePresence::Unset => point.m(),
                    ReplacePresence::Set(value) => value,
                };
                Shape::Point(Point::new_axes(x, y, ZOrdinate(z), MOrdinate(m))?)
            },
            _ => unreachable!("Point wraps a Point shape"),
        };

        let mut frame = base.frame.clone();
        if let ReplacePresence::Set(crs) = replace_crs(kwargs, "crs")? {
            frame = FrameEdit::SetCrs {
                crs,
                overwrite: true,
            }
            .apply(&frame)
            .map_err(PyErr::from)?;
        }
        if let ReplacePresence::Set(epoch) = replace_epoch(kwargs, "epoch")? {
            frame = FrameEdit::SetEpoch {
                epoch,
                overwrite: true,
            }
            .apply(&frame)
            .map_err(PyErr::from)?;
        }

        Ok(Typed(PyGeometry::with_frame(
            Arc::new(ShapeData::new(shape)),
            frame,
        )))
    }
}

#[pymethods]
impl PyLineString {
    /// Create a LineString from an ordered coordinate sequence.
    ///
    /// Parameters
    /// ----------
    /// coordinates : sequence, optional
    ///     Ordered ``(x, y[, z[, m]])`` tuples, or an ``(N, 2..4)`` array.
    ///     Mutually exclusive with the ``x``/``y`` column form. Omit all inputs
    ///     for an empty linestring.
    ///
    /// x, y : sequence of float, optional
    ///     Per-vertex X and Y ordinates as parallel columns, as an alternative to
    ///     ``coordinates``. Both are required together.
    ///
    /// z, m : sequence of float, optional
    ///     Per-vertex Z and M ordinates, as an alternative to inline tuples.
    ///
    /// crs : str or int, optional
    ///     CRS as an EPSG code or authority/WKT. Declares; no transform.
    ///
    /// epoch : float, optional
    ///     Coordinate epoch (decimal year) for time-dependent frames.
    ///
    /// Returns
    /// -------
    /// LineString
    ///     A linestring geometry — empty when no coordinates are given.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If coordinates are non-finite, ragged, or fewer than two vertices.
    /// CRSError
    ///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    /// GeometryError
    ///     If ``epoch`` is not a finite decimal year.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.LineString([(0, 0), (1, 1)]).to_wkt()
    /// 'LINESTRING (0 0, 1 1)'
    /// >>> gm.LineString().to_wkt()
    /// 'LINESTRING EMPTY'
    #[new]
    #[pyo3(
        signature = (coordinates = None, *, x = None, y = None, z = None, m = None, crs = None, epoch = None),
        text_signature = "(coordinates=None, *, x=None, y=None, z=None, m=None, crs=None, epoch=None)"
    )]
    fn new(
        coordinates: Option<&Bound<'_, PyAny>>,
        x: Option<&Bound<'_, PyAny>>,
        y: Option<&Bound<'_, PyAny>>,
        z: Option<&Bound<'_, PyAny>>,
        m: Option<&Bound<'_, PyAny>>,
        crs: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyClassInitializer<Self>> {
        Ok(leaf_initializer!(build_line_string(
            coordinates,
            x,
            y,
            z,
            m,
            crs,
            epoch
        )?))
    }
}

#[pymethods]
impl PyPolygon {
    /// Create a ``Polygon`` from an exterior ring and optional holes.
    ///
    /// Parameters
    /// ----------
    /// shell : sequence, optional
    ///     Exterior ring coordinates; closed automatically (needs ≥3 corners).
    ///     Mutually exclusive with the ``x``/``y`` column form.
    ///
    /// holes : sequence of sequence, optional
    ///     Interior ring (hole) coordinate sequences, each closed automatically.
    ///
    /// x, y : sequence of float, optional
    ///     Per-vertex X and Y ordinates for the exterior ring, as an alternative
    ///     to ``shell``. Both are required together.
    ///
    /// z, m : sequence of float, optional
    ///     Per-vertex Z and M ordinates for the exterior ring.
    ///
    /// crs : str or int, optional
    ///     CRS as an EPSG code or authority/WKT. Declares; no transform.
    ///
    /// epoch : float, optional
    ///     Coordinate epoch (decimal year) for time-dependent frames.
    ///
    /// Returns
    /// -------
    /// Polygon
    ///     A polygon geometry.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If a ring has fewer than three corners or any coordinate is non-finite.
    /// CRSError
    ///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    /// GeometryError
    ///     If ``epoch`` is not a finite decimal year.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]).to_wkt()
    /// 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
    /// >>> gm.Polygon().to_wkt()
    /// 'POLYGON EMPTY'
    #[new]
    #[pyo3(
        signature = (shell = None, holes = None, *, x = None, y = None, z = None, m = None, crs = None, epoch = None),
        text_signature = "(shell=None, holes=None, *, x=None, y=None, z=None, m=None, crs=None, epoch=None)"
    )]
    fn new(
        shell: Option<&Bound<'_, PyAny>>,
        holes: Option<&Bound<'_, PyAny>>,
        x: Option<&Bound<'_, PyAny>>,
        y: Option<&Bound<'_, PyAny>>,
        z: Option<&Bound<'_, PyAny>>,
        m: Option<&Bound<'_, PyAny>>,
        crs: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyClassInitializer<Self>> {
        Ok(leaf_initializer!(build_polygon(
            shell, holes, x, y, z, m, crs, epoch
        )?))
    }
}

#[pymethods]
impl PyMultiPoint {
    /// Create a ``MultiPoint`` geometry from a coordinate sequence.
    ///
    /// Parameters
    /// ----------
    /// coordinates : sequence, optional
    ///     Member coordinate tuples ``(x, y[, z[, m]])``. Mutually exclusive with
    ///     the ``x``/``y`` column form. Omit all inputs for an empty multipoint.
    ///
    /// x, y : sequence of float, optional
    ///     Per-point X and Y ordinates as parallel columns, as an alternative to
    ///     ``coordinates``. Both are required together.
    ///
    /// z, m : sequence of float, optional
    ///     Per-point Z and M ordinates, as an alternative to inline tuples.
    ///
    /// crs : str or int, optional
    ///     CRS as an EPSG code or authority/WKT. Declares; no transform.
    ///
    /// epoch : float, optional
    ///     Coordinate epoch (decimal year) for time-dependent frames.
    ///
    /// Returns
    /// -------
    /// MultiPoint
    ///     A multipoint geometry — empty when no coordinates are given.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If any coordinate is non-finite or has mixed dimensionality.
    /// CRSError
    ///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    /// GeometryError
    ///     If ``epoch`` is not a finite decimal year.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.MultiPoint([(0, 0), (1, 1)]).to_wkt()
    /// 'MULTIPOINT ((0 0), (1 1))'
    /// >>> gm.MultiPoint().to_wkt()
    /// 'MULTIPOINT EMPTY'
    #[new]
    #[pyo3(
        signature = (coordinates = None, *, x = None, y = None, z = None, m = None, crs = None, epoch = None),
        text_signature = "(coordinates=None, *, x=None, y=None, z=None, m=None, crs=None, epoch=None)"
    )]
    fn new(
        coordinates: Option<&Bound<'_, PyAny>>,
        x: Option<&Bound<'_, PyAny>>,
        y: Option<&Bound<'_, PyAny>>,
        z: Option<&Bound<'_, PyAny>>,
        m: Option<&Bound<'_, PyAny>>,
        crs: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyClassInitializer<Self>> {
        Ok(leaf_initializer!(build_multi_point(
            coordinates,
            x,
            y,
            z,
            m,
            crs,
            epoch
        )?))
    }
}

#[pymethods]
impl PyMultiLineString {
    /// Create a ``MultiLineString`` from a sequence of line coordinate sequences.
    ///
    /// Parameters
    /// ----------
    /// lines : sequence, optional
    ///     Each member is an ordered coordinate sequence (a line) or an
    ///     already-built ``LineString``. Omit for an empty multilinestring.
    ///
    /// crs : str or int, optional
    ///     CRS as an EPSG code or authority/WKT. Declares; no transform.
    ///
    /// epoch : float, optional
    ///     Coordinate epoch (decimal year) for time-dependent frames.
    ///
    /// Returns
    /// -------
    /// MultiLineString
    ///     A multilinestring geometry — empty when ``lines`` is omitted.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If a member line has fewer than two vertices or non-finite coordinates.
    /// CRSError
    ///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    /// GeometryError
    ///     If ``epoch`` is not a finite decimal year.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]]).to_wkt()
    /// 'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))'
    /// >>> gm.MultiLineString().to_wkt()
    /// 'MULTILINESTRING EMPTY'
    #[new]
    #[pyo3(
        signature = (lines = None, *, crs = None, epoch = None),
        text_signature = "(lines=None, *, crs=None, epoch=None)"
    )]
    fn new(
        lines: Option<&Bound<'_, PyAny>>,
        crs: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyClassInitializer<Self>> {
        Ok(leaf_initializer!(build_multi_line_string(
            lines, crs, epoch
        )?))
    }
}

#[pymethods]
impl PyMultiPolygon {
    /// Create a ``MultiPolygon`` from a sequence of polygons.
    ///
    /// Parameters
    /// ----------
    /// polygons : sequence, optional
    ///     Each member is ``[shell]`` or ``[shell, *holes]`` of coordinate rings,
    ///     or an already-built ``Polygon``. Omit for an empty multipolygon.
    ///
    /// crs : str or int, optional
    ///     CRS as an EPSG code or authority/WKT. Declares; no transform.
    ///
    /// epoch : float, optional
    ///     Coordinate epoch (decimal year) for time-dependent frames.
    ///
    /// Returns
    /// -------
    /// MultiPolygon
    ///     A multipolygon geometry — empty when ``polygons`` is omitted.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If any ring has fewer than three corners or non-finite coordinates.
    /// CRSError
    ///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    /// GeometryError
    ///     If ``epoch`` is not a finite decimal year.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> left = [[(0, 0), (1, 0), (1, 1)]]
    /// >>> right = [[(2, 2), (3, 2), (3, 3)]]
    /// >>> len(gm.MultiPolygon([left, right]).parts)
    /// 2
    /// >>> gm.MultiPolygon().to_wkt()
    /// 'MULTIPOLYGON EMPTY'
    #[new]
    #[pyo3(
        signature = (polygons = None, *, crs = None, epoch = None),
        text_signature = "(polygons=None, *, crs=None, epoch=None)"
    )]
    fn new(
        polygons: Option<&Bound<'_, PyAny>>,
        crs: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyClassInitializer<Self>> {
        Ok(leaf_initializer!(build_multi_polygon(
            polygons, crs, epoch
        )?))
    }
}

#[pymethods]
impl PyGeometryCollection {
    /// Create a ``GeometryCollection`` from a sequence of geometries.
    ///
    /// Parameters
    /// ----------
    /// geometries : sequence of Geometry, optional
    ///     Member geometries; may be of mixed types. Omit for an empty collection.
    ///
    /// crs : str or int, optional
    ///     CRS as an EPSG code or authority/WKT. Declares; no transform.
    ///
    /// epoch : float, optional
    ///     Coordinate epoch (decimal year) for time-dependent frames.
    ///
    /// Returns
    /// -------
    /// GeometryCollection
    ///     A geometry collection — empty when ``geometries`` is omitted.
    ///
    /// Raises
    /// ------
    /// TypeError
    ///     If any member is not a Geometry.
    /// CRSError
    ///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
    /// CRSMismatchError
    ///     If members carry conflicting CRS/epoch metadata.
    /// GeometryError
    ///     If ``epoch`` is not a finite decimal year.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gc = gm.GeometryCollection([gm.Point(0, 0), gm.Point(1, 1)])
    /// >>> gc.geometry_type
    /// 'GeometryCollection'
    /// >>> gm.GeometryCollection().to_wkt()
    /// 'GEOMETRYCOLLECTION EMPTY'
    #[new]
    #[pyo3(
        signature = (geometries = None, *, crs = None, epoch = None),
        text_signature = "(geometries=None, *, crs=None, epoch=None)"
    )]
    fn new(
        geometries: Option<&Bound<'_, PyAny>>,
        crs: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyClassInitializer<Self>> {
        Ok(leaf_initializer!(build_geometry_collection(
            geometries, crs, epoch
        )?))
    }
}
