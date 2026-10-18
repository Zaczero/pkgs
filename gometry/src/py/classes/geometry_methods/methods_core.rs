use pyo3::types::PyTupleMethods as _;

use crate::boundary::coordinates::measure_shape;
use crate::dispatch::{MetricOp, kernels};
use crate::predicates::unary;
use crate::py::classes::geometry_methods::{
    Bound, Py, PyAny, PyAnyMethods as _, PyDict, PyGeometry, PyResult, PyTuple, Python, Typed,
    pymethods,
};
use crate::py::support::geojson_domain_metric;
use crate::{
    Arc, Bounds, Frame, PyCoordinates, PyCrs, PyTypeError, Shape, coordinates,
    map_coordinates_callback, parse_coordinate_replacement, render_shape_html, render_shape_svg,
    require_geojson_crs,
};

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
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

    pub fn _repr_svg_(&self) -> PyResult<String> {
        Ok(render_shape_svg(&self.shape)?)
    }

    pub fn _repr_html_(&self, py: Python<'_>) -> PyResult<String> {
        let repr = self.__repr__(py)?;
        Ok(render_shape_html(&self.shape, &repr)?)
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
        self.crs_ref().cloned().map(PyCrs::from_definition)
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
    pub fn coordinate_axes(&self, py: Python<'_>) -> &'static str {
        if matches!(
            self.shape.shape(),
            Shape::Polygon(_) | Shape::GeometryCollection(_)
        ) {
            return py.detach(|| self.shape.coordinate_axes());
        }
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
    pub fn topological_dimension(&self, py: Python<'_>) -> u8 {
        if matches!(self.shape.shape(), Shape::GeometryCollection(_)) {
            return py.detach(|| self.shape.topological_dimension().code());
        }
        self.shape.topological_dimension().code()
    }
    /// Whether the geometry carries a Z ordinate.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn has_z(&self, py: Python<'_>) -> bool {
        if matches!(
            self.shape.shape(),
            Shape::Polygon(_) | Shape::GeometryCollection(_)
        ) {
            return py.detach(|| self.shape.has_z());
        }
        self.shape.has_z()
    }
    /// Whether the geometry carries an M ordinate.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn has_m(&self, py: Python<'_>) -> bool {
        if matches!(
            self.shape.shape(),
            Shape::Polygon(_) | Shape::GeometryCollection(_)
        ) {
            return py.detach(|| self.shape.has_m());
        }
        self.shape.has_m()
    }
    /// Axis-aligned bounds ``(minx, miny, maxx, maxy)``, or ``None`` if empty.
    /// Geographic bounds use periodic longitude; ``minx > maxx`` denotes an
    /// interval crossing that frame's seam. Empty rows in a `GeometryArray`
    /// are all-``nan`` instead — see `GeometryArray.bounds`.
    ///
    /// Returns
    /// -------
    /// tuple or None
    #[getter]
    pub fn bounds(&self, py: Python<'_>) -> Option<(f64, f64, f64, f64)> {
        if matches!(self.frame, Frame::None)
            && matches!(self.shape.shape(), Shape::Point(_) | Shape::Empty(..))
        {
            return self.shape.bounds().map(Bounds::into_tuple);
        }
        if matches!(self.frame, Frame::None)
            && let Some(bounds) = self.shape.existing_bounds()
        {
            return Some(bounds.into_tuple());
        }
        py.detach(|| {
            let raw = self.shape.bounds();
            if let Ok(crate::geometry::TopologyDomain::Geographic(frame)) =
                crate::geometry::topology_domain(&self.frame)
            {
                return crate::geographic::GeographicSupportProducts::public_bounds_for_shape(
                    frame,
                    self.shape.shape(),
                    raw,
                )
                .map(Bounds::into_tuple);
            }
            raw.map(Bounds::into_tuple)
        })
    }
    /// Indexable, iterable view over this geometry's vertices.
    ///
    /// Returns
    /// -------
    /// Coordinates
    ///     Flat, indexable view of vertex coordinates (X/Y and active Z/M).
    #[getter]
    pub fn coords(slf: &Bound<'_, Self>) -> PyCoordinates {
        let geometry = slf.borrow();
        PyCoordinates::new(
            coordinates::CoordinateView::from_shape(Arc::clone(&geometry.shape)),
            slf.clone().unbind().into_any(),
        )
    }
    /// Total number of coordinates in this geometry.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    pub fn num_coordinates<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, pyo3::types::PyInt>> {
        let measure = measure_shape(py, self.shape.shape())?;
        Ok(measure.count_pyint(py))
    }
    /// Return a geometry with selected coordinate ordinates replaced.
    ///
    /// Pass one ``(N, dims)`` matrix (including a `Coordinates` view), or
    /// replace any of ``x=``, ``y=``, ``z=``, and ``m=`` by a scalar or a
    /// column over the flattened coordinate sequence. Omitted ordinates are
    /// preserved. ``z=None`` and ``m=None`` remove those dimensions; X and Y
    /// cannot be removed. Matrix and column modes cannot be mixed. Calling
    /// without arguments returns an unchanged value.
    ///
    /// Parameters
    /// ----------
    /// coordinates : sequence of float, optional
    ///     Replacement ``(N, dims)`` coordinate matrix, including a
    ///     `Coordinates` view.
    /// x, y, z, m : float, sequence of float, or None, optional
    ///     Replace one ordinate by a scalar broadcast over vertices or a
    ///     column with one value per coordinate. Omitted ordinates are
    ///     preserved. ``None`` removes Z or M and is not valid for X or Y.
    ///
    /// Returns
    /// -------
    /// Geometry
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.LineString([(0, 0), (1, 1)]).with_coordinates([(5, 5), (6, 6)]).to_wkt()
    /// 'LINESTRING (5 5, 6 6)'
    #[pyo3(signature = (*args, **kwargs), text_signature = "($self, coordinates=None, /, *, x=..., y=..., z=..., m=...)")]
    pub fn with_coordinates(
        &self,
        py: Python<'_>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Typed> {
        let no_matrix = match args.len() {
            0 => true,
            1 => args.get_item(0)?.is_none(),
            _ => false,
        };
        if no_matrix && kwargs.is_none_or(pyo3::types::PyDictMethods::is_empty) {
            return Ok(Typed(self.clone()));
        }
        let measure = measure_shape(py, self.shape.shape())?;
        let axes = measure.shape_axes();
        let len = measure.len_for_python()?;
        let replacement = parse_coordinate_replacement(py, args, kwargs, axes, len)?;
        self.replace_coordinates_impl(py, replacement)
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
    pub fn map_coordinates(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        func: &Bound<'_, PyAny>,
    ) -> PyResult<Typed> {
        let geometry = slf.borrow();
        let coords = PyCoordinates::new(
            coordinates::CoordinateView::from_shape(Arc::clone(&geometry.shape)),
            slf.clone().unbind().into_any(),
        );
        let replacement = map_coordinates_callback(py, &coords, func)?;
        geometry.replace_coordinates_impl(py, replacement)
    }
    /// Whether the geometry is empty (no points, rings, or parts).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_empty(&self, py: Python<'_>) -> bool {
        if matches!(
            self.shape.shape(),
            Shape::Point(_) | Shape::Empty(..) | Shape::LineString(_) | Shape::MultiPoint(_)
        ) {
            return self.shape.is_empty();
        }
        unary::is_empty_scalar(py, self).expect("is_empty is infallible")
    }
    /// Number of top-level parts: ``1`` for a single point/line/polygon,
    /// including a typed empty; the member count for a multi/collection, and
    /// ``0`` for an empty multipart or collection. Explicit empty members
    /// still count in their multipart's member count.
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
    pub fn is_closed(&self, py: Python<'_>) -> bool {
        unary::is_closed_scalar(py, self).expect("is_closed is infallible")
    }
    /// Whether the geometry is a closed, simple ``LineString`` (a ring).
    /// Geographic closure uses quotient vertex identity and simplicity uses
    /// the frame-aware topology; projected and CRS-free geometry remains planar.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_ring(&self, py: Python<'_>) -> PyResult<bool> {
        unary::is_ring_scalar(py, self)
    }
    /// Whether a closed ``LineString`` winds counter-clockwise.
    /// ``False`` for open lines and non-lineal geometry.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_ccw(&self, py: Python<'_>) -> bool {
        unary::is_ccw_scalar(py, self).expect("is_ccw is infallible")
    }
    /// Whether the geometry has no self-intersections or self-tangencies.
    /// Repeated CONSECUTIVE vertices are removable redundancy, not a
    /// self-intersection, so they do not affect simplicity. Areal simplicity
    /// is validity: a polygon/multipolygon
    /// is simple exactly when it is ``is_valid`` — so holes outside the
    /// shell, nested holes, or a disconnected interior make it not simple
    /// even with no ring self-crossing. Collections are never simple.
    /// Geographic simplicity uses the CRS longitude quotient, so periodic
    /// aliases and seam cuts do not depend on their stored spelling. Geographic
    /// coordinate-admission failures still raise; projected and CRS-free
    /// geometry remains planar.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_simple(&self, py: Python<'_>) -> PyResult<bool> {
        unary::is_simple_scalar(py, self)
    }
    /// Whether the geometry is topologically valid in its coordinate frame.
    /// Geographic validity uses exact quotient topology, so period aliases and
    /// seam cuts do not depend on their stored longitude spelling. An
    /// out-of-domain latitude returns ``False`` and is reported by ``validate``;
    /// geographic operations that require coordinate admission still raise.
    /// Projected and CRS-free geometry uses ordinary planar OGC validity.
    /// A nonempty LineString needs at least two distinct XY positions;
    /// consecutive repeats remain valid when another position is present.
    /// ``True`` exactly when `validate` finds no issue; call
    /// `validate` for the reason, location, and path of a failure, and
    /// ``repair`` to fix it.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub fn is_valid(&self, py: Python<'_>) -> PyResult<bool> {
        unary::is_valid_scalar(py, self)
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
    pub fn is_convex(&self, py: Python<'_>) -> PyResult<bool> {
        unary::is_convex_scalar(py, self)
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
    pub fn crosses_antimeridian(&self, py: Python<'_>) -> PyResult<bool> {
        unary::crosses_antimeridian_scalar(py, self)
    }

    #[getter("__geo_interface__")]
    pub fn geo_interface<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        require_geojson_crs(self.crs_str())?;
        if self.has_m(py) || self.epoch().is_some() {
            return Err(PyTypeError::new_err(crate::io::geojson_interface_error()));
        }
        let shape = std::sync::Arc::clone(&self.shape);
        let mut metric_scratch = crate::dispatch::MetricScratch::default();
        let (domain, metric) = geojson_domain_metric(self.crs_str(), &mut metric_scratch)?;
        let metric_access = move || {
            metric.geographic_chart_metric()?.ok_or_else(|| {
                crate::crs::CrsError::invalid(
                    "geographic GeoJSON source ordinates require the geometry CRS metric",
                )
            })
        };
        let text = py.detach(move || {
            crate::io::to_geojson_string_with_metric(shape.shape(), domain, &metric_access)
        })?;
        let geometry = crate::boundary::json_text_to_py(py, &text)?;
        Ok(geometry.bind(py).clone().cast_into()?)
    }
    /// Area in CRS-natural units for the geometry's CRS.
    ///
    /// A geographic CRS gives ellipsoidal square meters for the region bounded
    /// by edges straight in longitude/latitude;
    /// a projected CRS gives squared native linear units; a CRS-free geometry
    /// gives squared raw coordinate units. Use ``to_crs`` to
    /// change frame. Geographic polygons measure their longitude/latitude chart
    /// interior, including regions larger than half the ellipsoid.
    ///
    /// A ``GeometryCollection`` counts overlapping members once, as the area
    /// of their union. Parts of a valid ``MultiPolygon`` cannot overlap.
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
    pub fn area(&self, py: Python<'_>) -> PyResult<f64> {
        crate::area_natural_scalar(py, self)
    }
    /// Length (curves) or perimeter (areal), measured for the geometry's CRS.
    ///
    /// A geographic CRS gives ellipsoidal meters along edges straight in
    /// longitude/latitude; a projected CRS gives native linear units; a CRS-free
    /// geometry gives raw coordinate units. Use ``to_crs`` to change frame.
    ///
    /// Geographic polygon perimeter measures straight chart edges and resolves
    /// seam aliases in the CRS-native periodic longitude frame.
    ///
    /// A ``GeometryCollection`` sums every member's length or perimeter,
    /// including overlapping members.
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
    pub fn length(&self, py: Python<'_>) -> PyResult<f64> {
        crate::length_natural_scalar(py, self)
    }
    /// 3D length of curves with Z, measured for the geometry's CRS.
    ///
    /// Geographic CRSs do not support 3D Euclidean length.
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
    pub fn length_3d(&self, py: Python<'_>) -> PyResult<f64> {
        crate::dispatch::unary_scalar(
            py,
            self,
            crate::dispatch::Operation::Metric(MetricOp::Length3d),
            None,
            kernels::unary_length_3d,
        )
    }
    /// Smallest Z ordinate, or ``None`` if no vertex carries Z.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub fn min_z(&self, py: Python<'_>) -> Option<f64> {
        if !matches!(
            self.shape.shape(),
            Shape::Polygon(_) | Shape::GeometryCollection(_)
        ) && !self.shape.has_z()
        {
            return None;
        }
        if matches!(self.shape.shape(), Shape::Point(_) | Shape::Empty(..)) {
            return self.shape.min_z();
        }
        py.detach(|| self.shape.min_z())
    }
    /// Largest Z ordinate, or ``None`` if no vertex carries Z.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub fn max_z(&self, py: Python<'_>) -> Option<f64> {
        if !matches!(
            self.shape.shape(),
            Shape::Polygon(_) | Shape::GeometryCollection(_)
        ) && !self.shape.has_z()
        {
            return None;
        }
        if matches!(self.shape.shape(), Shape::Point(_) | Shape::Empty(..)) {
            return self.shape.max_z();
        }
        py.detach(|| self.shape.max_z())
    }
    /// Smallest M ordinate, or ``None`` if no vertex carries M.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub fn min_m(&self, py: Python<'_>) -> Option<f64> {
        if !matches!(
            self.shape.shape(),
            Shape::Polygon(_) | Shape::GeometryCollection(_)
        ) && !self.shape.has_m()
        {
            return None;
        }
        if matches!(self.shape.shape(), Shape::Point(_) | Shape::Empty(..)) {
            return self.shape.min_m();
        }
        py.detach(|| self.shape.min_m())
    }
    /// Largest M ordinate, or ``None`` if no vertex carries M.
    ///
    /// Returns
    /// -------
    /// float or None
    #[getter]
    pub fn max_m(&self, py: Python<'_>) -> Option<f64> {
        if !matches!(
            self.shape.shape(),
            Shape::Polygon(_) | Shape::GeometryCollection(_)
        ) && !self.shape.has_m()
        {
            return None;
        }
        if matches!(self.shape.shape(), Shape::Point(_) | Shape::Empty(..)) {
            return self.shape.max_m();
        }
        py.detach(|| self.shape.max_m())
    }
}

#[cfg(test)]
mod extent_tests {
    use super::*;
    use crate::crs::frame::crs_arc_static;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn ordinate_extents_preserve_absent_axes_empty_and_nested_values() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            for (wkt, expected) in [
                ("POINT ZM (1 2 3 4)", [
                    Some(3.0),
                    Some(3.0),
                    Some(4.0),
                    Some(4.0),
                ]),
                ("POINT ZM EMPTY", [None; 4]),
                ("MULTILINESTRING ((0 0, 1 1), (3 4, 5 6))", [None; 4]),
                (
                    "GEOMETRYCOLLECTION (POINT Z (1 2 9), LINESTRING M (3 4 -2, 5 6 7))",
                    [Some(9.0), Some(9.0), Some(-2.0), Some(7.0)],
                ),
            ] {
                let geometry = PyGeometry::with_frame(
                    crate::io::parse_wkt(wkt).expect("valid geometry"),
                    Frame::None,
                );
                assert_eq!(
                    [
                        geometry.min_z(py),
                        geometry.max_z(py),
                        geometry.min_m(py),
                        geometry.max_m(py),
                    ],
                    expected,
                    "{wkt}"
                );
            }
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn scalar_bounds_keep_cached_cartesian_and_periodic_geographic_results() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let shape = crate::io::parse_wkt("LINESTRING (179 -2, 181 4)").unwrap();
            for (frame, expected) in [
                (Frame::None, (179.0, -2.0, 181.0, 4.0)),
                (
                    Frame::Crs(crs_arc_static("EPSG:3857")),
                    (179.0, -2.0, 181.0, 4.0),
                ),
                (
                    Frame::Crs(crs_arc_static("OGC:CRS84")),
                    (179.0, -2.0, -179.0, 4.0),
                ),
            ] {
                let geometry = PyGeometry::with_frame(shape.clone(), frame);
                assert_eq!(geometry.bounds(py), Some(expected));
                assert_eq!(geometry.bounds(py), Some(expected));
            }
        });
    }
}
