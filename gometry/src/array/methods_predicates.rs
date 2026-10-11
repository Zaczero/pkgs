use numpy::PyArrayMethods as _;
use pyo3::IntoPyObjectExt as _;

use crate::array::{
    ArrayRow, ArrowEncoding, Bound, F64Param, GeometryError, Py, PyAny, PyAnyMethods as _,
    PyGeometry, PyGeometryArray, PyResult, PyValueError, Python, SimplifyMethod, Typed, pymethods,
};
use crate::geometry::storage::{GeometryArrayStorage, PackedStorage};
use crate::py::functions::polyline;
use crate::py::numpy::numpy_module;
use crate::py::{arrow, arrow_c, errors};

impl PyGeometryArray {
    pub(crate) fn materialized_objects(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        use pyo3::IntoPyObjectExt as _;
        self.row_operation()
            .rows_with_missing(|| ())
            .map(|row| match row {
                ArrayRow::Missing { .. } => Ok(py.None()),
                ArrayRow::Present(row) => {
                    let geometry = PyGeometry::with_frame(
                        row.row().with_shape(Clone::clone)?,
                        self.frame.clone(),
                    );
                    Typed(geometry).into_py_any(py)
                },
            })
            .collect()
    }
}

#[pymethods]
impl PyGeometryArray {
    /// Export the array as a ``numpy.ndarray`` of typed geometry objects.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One typed leaf geometry per row.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.GeometryArray([gm.Point(1, 2)]).to_numpy()[0].to_wkt()
    /// 'POINT (1 2)'
    pub fn to_numpy(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let len = self.len();
        let mut values = Vec::with_capacity(len);
        let mut rows = self.row_operation();
        for row in rows.rows_with_missing(|| ()) {
            match row {
                ArrayRow::Missing { .. } => values.push(py.None()),
                ArrayRow::Present(row) => {
                    let geometry = Typed(rows.geometry(row).map_err(crate::PyErr::from)?);
                    values.push(geometry.into_py_any(py)?);
                },
            }
        }
        let array =
            numpy::PyArray1::from_owned_object_array(py, numpy::ndarray::Array1::from_vec(values));
        array.try_readwrite()?.make_nonwriteable();
        Ok(array.into_any().unbind())
    }

    #[pyo3(signature = (dtype=None, copy=None))]
    pub fn __array__(
        &self,
        py: Python<'_>,
        dtype: Option<&Bound<'_, PyAny>>,
        copy: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        if copy == Some(false) {
            return Err(PyValueError::new_err(
                "gometry cannot return the requested array without copying",
            ));
        }
        if let Some(dtype) = dtype
            && !dtype.is_none()
        {
            let numpy = numpy_module(py)?;
            let dtype = numpy.getattr("dtype")?.call1((dtype,))?;
            if dtype.getattr("kind")?.extract::<String>()? != "O" {
                return Err(GeometryError::new_err("dtype must be object or None"));
            }
        }
        self.to_numpy(py)
    }
    /// Export the array as a `GeoArrow` array.
    ///
    /// Parameters
    /// ----------
    /// encoding : {'auto', 'wkb'}, default auto
    ///     ``auto`` exports homogeneous arrays as their native GeoArrow layout
    ///     and falls back to WKB for mixed geometry types; ``wkb`` always
    ///     exports a GeoArrow WKB array.
    ///
    /// Returns
    /// -------
    /// object
    ///     A GeoArrow-compatible array.
    ///
    /// See Also
    /// --------
    /// from_arrow : Decode a GeoArrow array into a ``GeometryArray``.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> type(gm.GeometryArray([gm.Point(1, 2)]).to_arrow()).__name__
    /// 'ExtensionArray'
    #[pyo3(signature = (*, encoding = ArrowEncoding::Auto), text_signature = "($self, *, encoding='auto')")]
    pub fn to_arrow(&self, py: Python<'_>, encoding: ArrowEncoding) -> PyResult<Py<PyAny>> {
        arrow::array_to_arrow(py, self, encoding)
    }

    pub fn __arrow_c_schema__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        arrow_c::array_to_schema_capsule(py, self)
    }

    #[pyo3(signature = (requested_schema = None))]
    pub fn __arrow_c_array__(
        &self,
        py: Python<'_>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        arrow_c::array_to_array_capsules(py, self, requested_schema)
    }

    #[pyo3(signature = (requested_schema = None))]
    pub fn __arrow_c_stream__(
        &self,
        py: Python<'_>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        arrow_c::array_to_stream_capsule(py, self, requested_schema)
    }

    /// Encode every ``LineString`` or ``Point`` row as Google polyline text (see
    /// `Geometry.to_polyline`).
    #[doc = crate::writer_ordinate_policy!()]
    ///
    /// Parameters
    /// ----------
    /// precision : int, default 5
    ///     Decimal digits encoded per ordinate (``0`` to ``11``).
    /// drop_epoch : bool, default False
    ///     Permit losing coordinate-epoch metadata, which polyline cannot
    ///     encode.
    ///
    /// Returns
    /// -------
    /// list of str or None
    ///     One encoded polyline per row, with ``None`` at missing rows.
    ///
    /// Raises
    /// ------
    /// GeometryTypeError
    ///     If a row is not a ``LineString`` or ``Point``.
    /// CRSError
    ///     If the CRS is set and is not EPSG:4326 longitude/latitude.
    /// InvalidGeometryError
    ///     If a row carries Z/M, or a coordinate is outside the
    ///     longitude/latitude domain. Flatten explicitly with ``force_2d()``.
    /// GeometryError
    ///     If ``precision`` is out of range, or the array carries a coordinate
    ///     epoch and ``drop_epoch`` is false.
    ///
    /// See Also
    /// --------
    /// from_polyline : Decode Google polyline text into geometries.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.GeometryArray([gm.LineString([(0, 0), (1, 1)])]).to_polyline()
    /// ['??_ibE_ibE']
    #[pyo3(signature = (*, precision = 5, drop_epoch = false))]
    pub fn to_polyline(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = crate::strict_i32)] precision: i32,
        drop_epoch: bool,
    ) -> PyResult<Py<PyAny>> {
        errors::require_epoch_drop(self.epoch(), drop_epoch, "to_polyline")?;
        let factor = polyline::polyline_precision_factor(precision)?;
        let rows = py.detach(|| polyline::present_polylines_of(self, factor))?;
        self.masked_present_row_list(py, rows)
    }
}

impl PyGeometryArray {
    pub(crate) fn simplify_unary_packed(
        &self,
        tolerance: &F64Param,
        method: SimplifyMethod,
        preserve_topology: bool,
    ) -> Option<PyResult<Self>> {
        if let Some(identity) = self.packed_points_identity() {
            return Some(Ok(identity));
        }
        // Packed lines run the keep-mask kernel straight over the shared
        // columns and append survivors into NEW CSR columns — no per-row
        // Shape synthesis, no map + re-pack scan. (Topology preservation
        // adds the guard machinery, and a per-element tolerance varies the
        // mask per row: both keep the per-shape route.)
        if !preserve_topology
            && let Some(tolerance) = tolerance.as_scalar()
            && let GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords,
                offsets,
                row_map,
            }) = self.storage()
        {
            let longitude_axis = match crate::geometry::topology_domain(&self.frame) {
                Ok(crate::geometry::TopologyDomain::Geographic(frame)) => Some(frame.longitude()),
                Ok(crate::geometry::TopologyDomain::Cartesian) => None,
                Err(error) => return Some(Err(error.into())),
            };
            // Propagate real PyResult errors from the packed kernels — never
            // `.ok()` them into a silent fallthrough that hides the failure.
            if row_map.reorders() {
                let materialized = match self.materialize_packed_lines_parts(
                    coords,
                    offsets,
                    row_map.as_deref(),
                ) {
                    Ok(m) => m,
                    Err(err) => return Some(Err(err)),
                };
                if let GeometryArrayStorage::Packed(PackedStorage::Lines {
                    coords, offsets, ..
                }) = materialized.storage()
                {
                    return Some(materialized.simplify_packed_lines_with_method(
                        coords,
                        offsets,
                        tolerance,
                        method,
                        longitude_axis,
                    ));
                }
                return None;
            }
            return Some(self.simplify_packed_lines_with_method(
                coords,
                offsets,
                tolerance,
                method,
                longitude_axis,
            ));
        }
        None
    }
}
