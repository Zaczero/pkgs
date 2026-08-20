#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::Bounds3D;
use crate::py::classes::geometry_methods::{
    Bound, Py, PyAny, PyDict, PyGeometry, PyResult, PyTuple, Python, Typed, pymethods,
};
use crate::{
    Arc, Bounds, PyCoordinates, PyCrs, PyTypeError, coordinates, geojson_dict,
    map_coordinates_callback, parse_coordinate_replacement, render_shape_svg,
    replace_shape_coordinates,
};

#[pymethods]
impl PyGeometry {
    #[new]
    #[pyo3(text_signature = "()")]
    fn new() -> PyResult<Self> {
        Err(PyTypeError::new_err(
            "Geometry is abstract; construct a typed subclass such as Point or LineString",
        ))
    }

    // NEP 13: opt out of numpy ufunc dispatch (we have our own & | - ^ /
    // predicates)
    #[classattr]
    #[expect(non_upper_case_globals, reason = "Python dunder name")]
    const __array_ufunc__: Option<Py<PyAny>> = None;

    pub fn _repr_svg_(&self) -> String {
        render_shape_svg(&self.shape)
    }

    pub fn _repr_html_(&self) -> String {
        format!(
            "<div class=\"gometry-geom-html\"><div>{}</div>{}</div>",
            self.__repr__(),
            render_shape_svg(&self.shape)
        )
    }
    /// OGC geometry type name, e.g. ``'Point'`` or ``'MultiPolygon'``.
    ///
    /// Returns
    /// -------
    /// str
    ///     One of ``'Point'``, ``'LineString'``, ``'Polygon'``,
    ///     ``'MultiPoint'``, ``'MultiLineString'``, ``'MultiPolygon'``,
    ///     ``'GeometryCollection'``.
    #[getter]
    pub fn geometry_type(&self) -> &'static str {
        self.shape.geometry_type()
    }
    /// CRS attached to this geometry, or ``None``.
    ///
    /// Returns
    /// -------
    /// CRS or None
    #[getter]
    pub fn crs(&self) -> Option<PyCrs> {
        self.crs_ref().cloned().map(PyCrs::from_canonical)
    }
    /// Coordinate epoch of this geometry, if set.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub const fn epoch(&self) -> Option<f64> {
        self.frame.epoch()
    }
    /// Ordinate layout: ``'XY'``, ``'XYZ'``, ``'XYM'``, or ``'XYZM'``.
    /// This is the coordinate *layout* (which ordinates are present), not the
    /// topological dimension (see `topological_dimension`).
    ///
    /// Returns
    /// -------
    /// str
    ///     The geometry's exact ``'XY'``/``'XYZ'``/``'XYM'``/``'XYZM'``
    ///     coordinate layout.
    #[getter]
    pub fn coordinate_axes(&self) -> &'static str {
        self.shape.coordinate_axes()
    }
    /// Topological dimension: ``0`` (point), ``1`` (curve), or ``2`` (surface).
    /// The maximum over members for collections. Distinct from the coordinate
    /// layout (see `coordinate_axes`).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    pub fn topological_dimension(&self) -> u8 {
        self.shape.topological_dimension().code()
    }
    /// Whether the geometry carries a Z ordinate.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn has_z(&self) -> bool {
        self.shape.has_z()
    }
    /// Whether the geometry carries an M ordinate.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn has_m(&self) -> bool {
        self.shape.has_m()
    }
    /// Axis-aligned bounds ``(minx, miny, maxx, maxy)``, or ``None`` if empty.
    /// Empty rows in a `GeometryArray` are all-``nan`` instead — see
    /// `GeometryArray.bounds`.
    ///
    /// Returns
    /// -------
    /// tuple or None
    #[getter]
    pub fn bounds(&self) -> Option<(f64, f64, f64, f64)> {
        if crate::geometry::geographic_crossing(&self.frame, &self.shape) {
            return crate::geometry::geographic_crossing_bounds(self.shape.shape())
                .map(Bounds::into_tuple);
        }
        self.shape.bounds().map(Bounds::into_tuple)
    }
    /// Coordinate view over this geometry's vertices (storage-shaped index /
    /// cursor iteration — not an eagerly materialized vertex list).
    ///
    /// Returns
    /// -------
    /// Coordinates
    ///     Flat, indexable view of vertex coordinates (X/Y and active Z/M).
    #[getter]
    pub fn coords(&self) -> PyCoordinates {
        PyCoordinates::new(coordinates::CoordinateView::from_shape(Arc::clone(
            &self.shape,
        )))
    }
    /// Total number of coordinates in this geometry.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    pub fn num_coordinates(&self) -> usize {
        self.shape.coord_count()
    }
    /// Return a geometry with the same topology and replacement coordinates.
    ///
    /// Pass one ``(N, dims)`` matrix (including a `Coordinates` view) or
    /// explicit ``x=`` and ``y=`` columns. The vertex count and ordinate
    /// layout are preserved; use dimension setters for adding or removing
    /// Z/M axes.
    ///
    /// Parameters
    /// ----------
    /// coordinates : sequence of float, optional
    ///     Replacement ``(N, dims)`` coordinate matrix, including a
    ///     `Coordinates` view.
    /// x, y : sequence of float, optional
    ///     Replacement X and Y columns.
    /// z, m : sequence of float, optional
    ///     Replacement Z and M columns when this geometry already has those
    ///     axes. Omitted axes are carried unchanged; ``None`` is not a
    ///     clearing sentinel.
    ///
    /// Returns
    /// -------
    /// Geometry
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.LineString([(0, 0), (1, 1)]).set_coordinates([(5, 5), (6, 6)]).to_wkt()
    /// 'LINESTRING (5 5, 6 6)'
    #[pyo3(signature = (*args, **kwargs), text_signature = "($self, coordinates=None, /, *, x=..., y=..., z=..., m=...)")]
    pub fn set_coordinates(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Typed> {
        let replacement = parse_coordinate_replacement(
            py,
            args,
            kwargs,
            self.shape.axes(),
            self.shape.coord_count(),
        )?;
        let shape = replace_shape_coordinates(self.shape.shape(), &replacement)?;
        Ok(self.typed_shape(shape))
    }
    /// Apply a vectorized callback to this geometry's coordinate matrix.
    ///
    /// The callback receives a read-only ``(N, dims)`` float64 matrix and must
    /// return a matrix with the same shape. Topology, CRS, epoch, and ordinate
    /// layout are preserved.
    ///
    /// Parameters
    /// ----------
    /// func : callable
    ///     Function called with the read-only coordinate matrix.
    ///
    /// Returns
    /// -------
    /// Geometry
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.LineString([(0, 0), (1, 1)]).map_coordinates(lambda m: m + 1).to_wkt()
    /// 'LINESTRING (1 1, 2 2)'
    pub fn map_coordinates(&self, py: Python<'_>, func: &Bound<'_, PyAny>) -> PyResult<Typed> {
        let coords = PyCoordinates::new(coordinates::CoordinateView::from_shape(Arc::clone(
            &self.shape,
        )));
        let replacement = map_coordinates_callback(py, coords, func)?;
        let shape = replace_shape_coordinates(self.shape.shape(), &replacement)?;
        Ok(self.typed_shape(shape))
    }
    /// Whether the geometry is empty (no points, rings, or parts).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_empty(&self) -> bool {
        Python::attach(|py| crate::predicates::unary::is_empty_scalar(py, self))
            .expect("is_empty is infallible")
    }
    /// Number of top-level parts: ``1`` for a single point/line/polygon,
    /// the member count for a multi/collection, ``0`` for empty — the
    /// ``O(1)`` counterpart to ``len(geoms)`` without materializing parts.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    pub fn num_geometries(&self) -> usize {
        self.shape.part_count()
    }
    /// Whether every (multi)linestring component starts and ends at the same
    /// point.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_closed(&self) -> bool {
        Python::attach(|py| crate::predicates::unary::is_closed_scalar(py, self))
            .expect("is_closed is infallible")
    }
    /// Whether the geometry is a closed, simple ``LineString`` (a ring).
    /// Geographic antimeridian crossings are normalized before the simplicity
    /// test; projected and CRS-free geometry remains planar.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_ring(&self) -> bool {
        Python::attach(|py| crate::predicates::unary::is_ring_scalar(py, self))
            .expect("is_ring is infallible")
    }
    /// Whether a closed ``LineString`` winds counter-clockwise.
    /// ``False`` for open lines and non-lineal geometry.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_ccw(&self) -> bool {
        Python::attach(|py| crate::predicates::unary::is_ccw_scalar(py, self))
            .expect("is_ccw is infallible")
    }
    /// Whether the geometry has no self-intersections or self-tangencies.
    /// Repeated CONSECUTIVE vertices are removable redundancy, not a
    /// self-intersection, so they do not affect simplicity. Areal simplicity
    /// is validity: a polygon/multipolygon
    /// is simple exactly when it is ``is_valid`` — so holes outside the
    /// shell, nested holes, or a disconnected interior make it not simple
    /// even with no ring self-crossing. Collections are never simple.
    /// Geographic antimeridian crossings are normalized before topology is
    /// evaluated; projected and CRS-free geometry remains planar.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_simple(&self) -> bool {
        Python::attach(|py| crate::predicates::unary::is_simple_scalar(py, self))
            .expect("is_simple is infallible")
    }
    /// Whether the geometry is topologically valid in its coordinate frame.
    /// Geographic antimeridian crossings are normalized first; projected and
    /// CRS-free geometry uses ordinary planar OGC validity.
    /// ``True`` exactly when `validate` finds no issue; call
    /// `validate` for the reason, location, and path of a failure, and
    /// ``repair`` to fix it.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_valid(&self) -> bool {
        Python::attach(|py| crate::predicates::unary::is_valid_scalar(py, self))
            .expect("is_valid is infallible")
    }
    /// Whether the polygon is convex.
    ///
    /// Every shell turn has one orientation — collinear edges allowed —
    /// and there are no holes; the empty polygon is convex. Non-polygon
    /// geometries return ``False``.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_convex(&self) -> PyResult<bool> {
        Python::attach(|py| crate::predicates::unary::is_convex_scalar(py, self))
    }
    /// Whether a geographic geometry crosses the antimeridian.
    ///
    /// Returns
    /// -------
    /// bool
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the CRS is projected (a geographic CRS or CRS-free lon/lat is
    ///     required).
    #[getter]
    pub fn crosses_antimeridian(&self) -> PyResult<bool> {
        Python::attach(|py| crate::predicates::unary::crosses_antimeridian_scalar(py, self))
    }

    #[getter("__geo_interface__")]
    pub fn geo_interface<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        if self.has_m() || self.epoch().is_some() {
            return Err(PyTypeError::new_err(
                "__geo_interface__ cannot represent M or coordinate epoch; use an explicit serializer or clear the dimension",
            ));
        }
        geojson_dict(py, &self.shape)
    }
    /// Area in CRS-natural units for the geometry's CRS.
    ///
    /// A geographic CRS gives ellipsoidal square meters (geodesic, on the CRS's
    /// own ellipsoid); a projected CRS gives squared native coordinate units;
    /// a CRS-free geometry gives squared coordinate units. Use ``to_crs`` to
    /// change frame.
    ///
    /// Returns
    /// -------
    /// float
    ///     The area; ``0`` for non-areal geometries.
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the CRS lacks linear axis units for a metric result.
    ///
    /// See Also
    /// --------
    /// length : Length/perimeter under the same CRS-aware metric.
    #[getter]
    pub fn area(&self) -> PyResult<f64> {
        Python::attach(|py| crate::area_natural_scalar(py, self))
    }
    /// Length (curves) or perimeter (areal), measured for the geometry's CRS.
    ///
    /// A geographic CRS gives ellipsoidal meters (geodesic, on the CRS's own
    /// ellipsoid); a projected CRS gives native linear units; a CRS-free
    /// geometry gives coordinate units. Use ``to_crs`` to change frame.
    ///
    /// Returns
    /// -------
    /// float
    ///     The length or perimeter; ``0`` for points.
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the CRS lacks linear axis units for a metric result.
    ///
    /// See Also
    /// --------
    /// area : Area under the same CRS-aware metric.
    #[getter]
    pub fn length(&self) -> PyResult<f64> {
        Python::attach(|py| crate::length_natural_scalar(py, self))
    }
    /// 3D length of curves with Z, measured for the geometry's CRS.
    ///
    /// Returns
    /// -------
    /// float
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the CRS lacks linear axis units for a metric result.
    /// InvalidGeometryError
    ///     If the geometry lacks Z on every vertex.
    #[getter]
    pub fn length_3d(&self) -> PyResult<f64> {
        Python::attach(|py| {
            crate::dispatch::unary_scalar(
                py,
                self,
                crate::dispatch::Operation::Length3d,
                None,
                crate::dispatch::kernels::unary_length_3d,
            )
        })
    }
    /// Smallest Z ordinate, or ``None`` if no vertex carries Z.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub fn min_z(&self) -> Option<f64> {
        self.shape.min_z()
    }
    /// Largest Z ordinate, or ``None`` if no vertex carries Z.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub fn max_z(&self) -> Option<f64> {
        self.shape.max_z()
    }
    /// Span of Z ordinates (``max_z - min_z``), or ``None`` without Z.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub fn z_range(&self) -> Option<f64> {
        self.shape.z_extremes().map(|(low, high)| high - low)
    }
    /// Smallest M ordinate, or ``None`` if no vertex carries M.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub fn min_m(&self) -> Option<f64> {
        self.shape.min_m()
    }
    /// Largest M ordinate, or ``None`` if no vertex carries M.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub fn max_m(&self) -> Option<f64> {
        self.shape.max_m()
    }
    /// Span of M ordinates (``max_m - min_m``), or ``None`` without M.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub fn m_range(&self) -> Option<f64> {
        self.shape.m_extremes().map(|(low, high)| high - low)
    }
    /// 3D bounding box ``(minx, miny, minz, maxx, maxy, maxz)``.
    ///
    /// Returns
    /// -------
    /// tuple of float or None
    ///     ``None`` when the geometry is empty or carries no Z ordinate.
    #[getter]
    pub fn bounds_3d(&self) -> Option<(f64, f64, f64, f64, f64, f64)> {
        self.shape.bounds_3d().map(Bounds3D::into_tuple)
    }
}
