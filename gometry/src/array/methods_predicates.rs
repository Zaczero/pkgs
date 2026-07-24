#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use numpy::PyArrayMethods;

use super::*;

impl PyGeometryArray {
    pub(crate) fn materialized_objects(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        use pyo3::IntoPyObjectExt;
        self.masked_shape_rows()
            .map(|(missing, shape)| {
                if missing {
                    Ok(py.None())
                } else {
                    let geometry = PyGeometry::with_frame(shape.into_owned(), self.frame.clone());
                    Typed(geometry).into_py_any(py)
                }
            })
            .collect()
    }
}

fn to_arrow_dense(
    array: &PyGeometryArray,
    py: Python<'_>,
    encoding: ArrowEncoding,
) -> PyResult<Py<PyAny>> {
    if encoding == ArrowEncoding::Wkb {
        // Packed storage has one frame by construction; only Mixed storage can
        // carry per-item frame drift that needs the defensive compatibility
        // check. Avoiding `items()` here is load-bearing: on packed arrays it
        // would synthesize one `PyGeometry` for every row before WKB export.
        if let GeometryArrayStorage::Mixed(items) = array.storage() {
            // Empty framed Mixed: array CRS is authoritative (no elements).
            if !items.is_empty() {
                let crs = common_crs_required(items, "Arrow export")?;
                if crs.as_deref() != array.crs_str() {
                    return Err(crate::py::errors::crs_mismatch_error(
                        "Arrow export requires GeometryArray CRS metadata to match every element",
                        array.crs_str(),
                        crs.as_deref(),
                        None,
                    ));
                }
            }
        }
        return crate::py::arrow::storage_to_wkb_arrow(
            py,
            array.storage(),
            array.crs_str(),
            array.epoch(),
        );
    }
    if let GeometryArrayStorage::Points { coords, row_map } = array.storage() {
        if row_map.reorders() {
            return to_arrow_dense(&array.materialize_packed_points(), py, encoding);
        }
        return packed_points_to_arrow(py, coords, array.crs_str(), array.epoch());
    }
    if let GeometryArrayStorage::Lines {
        coords,
        offsets,
        row_map,
    } = array.storage()
    {
        if row_map.reorders() {
            return to_arrow_dense(&array.materialize_packed_lines()?, py, encoding);
        }
        return crate::py::arrow::packed_lines_to_arrow(
            py,
            coords,
            offsets,
            array.crs_str(),
            array.epoch(),
        );
    }
    if let GeometryArrayStorage::Polygons {
        coords,
        ring_offsets,
        polygon_offsets,
        row_map,
    } = array.storage()
    {
        if row_map.reorders() {
            return to_arrow_dense(&array.materialize_packed_polygons()?, py, encoding);
        }
        return crate::py::arrow::packed_polygons_to_arrow(
            py,
            coords,
            ring_offsets,
            polygon_offsets,
            array.crs_str(),
            array.epoch(),
        );
    }
    let items = array.items();
    // Shared empty/non-empty Mixed frame gate (zero-batch import re-export).
    let _ = crate::py::arrow_c::array_frame_crs(array, items.as_ref())?;
    geometries_to_arrow(py, items.as_ref(), array.crs_str(), array.epoch())
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
        let len = self.storage().len();
        let mut values = Vec::with_capacity(len);
        for index in 0..len {
            if self.is_row_missing(index) {
                values.push(py.None());
            } else {
                let geometry = Typed(self.storage().geometry_at(
                    index,
                    self.frame.clone(),
                    self.row_frame_cache(index),
                ));
                values.push(geometry.into_py_any(py)?);
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
        let _ = dtype;
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
    #[pyo3(signature = (*, encoding = ArrowEncoding::Auto), text_signature = "($self, *, encoding='auto')")]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> type(gm.GeometryArray([gm.Point(1, 2)]).to_arrow()).__name__
    /// 'ExtensionArray'
    pub fn to_arrow(&self, py: Python<'_>, encoding: ArrowEncoding) -> PyResult<Py<PyAny>> {
        if let Some(mask) = self.missing() {
            let dense = to_arrow_dense(self, py, encoding)?;
            let validity = crate::py::arrow::validity_bitmap_from_missing(mask);
            return Ok(crate::py::arrow::gometry_arrow_module(py)?
                .call_method1(
                    "apply_missing",
                    (dense, pyo3::types::PyBytes::new(py, &validity)),
                )?
                .unbind());
        }
        to_arrow_dense(self, py, encoding)
    }

    pub fn __arrow_c_schema__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::py::arrow_c::array_to_schema_capsule(py, self)
    }

    #[pyo3(signature = (requested_schema = None))]
    pub fn __arrow_c_array__(
        &self,
        py: Python<'_>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        crate::py::arrow::reject_requested_schema(requested_schema)?;
        crate::py::arrow_c::array_to_array_capsules(py, self)
    }

    #[pyo3(signature = (requested_schema = None))]
    pub fn __arrow_c_stream__(
        &self,
        py: Python<'_>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        crate::py::arrow::reject_requested_schema(requested_schema)?;
        crate::py::arrow_c::array_to_stream_capsule(py, self)
    }

    /// Encode every ``LineString`` or ``Point`` row as Google polyline text (see
    /// `Geometry.to_polyline`).
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
    ///     If ``precision`` is out of range.
    ///
    /// See Also
    /// --------
    /// from_polyline : Decode Google polyline text into geometries.
    #[pyo3(signature = (*, precision = 5, drop_epoch = false))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.GeometryArray([gm.LineString([(0, 0), (1, 1)])]).to_polyline()
    /// ['??_ibE_ibE']
    pub fn to_polyline(
        &self,
        py: Python<'_>,
        precision: i32,
        drop_epoch: bool,
    ) -> PyResult<Py<PyAny>> {
        crate::py::errors::require_epoch_drop(self.epoch(), drop_epoch, "to_polyline")?;
        let factor = crate::py::functions::polyline::polyline_precision_factor(precision)?;
        let rows = crate::py::functions::polyline::present_polylines_of(self, factor)?;
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
            && let GeometryArrayStorage::Lines {
                coords,
                offsets,
                row_map,
            } = self.storage()
        {
            let result = if row_map.reorders() {
                let materialized =
                    self.materialize_packed_lines_parts(coords, offsets, row_map.as_deref());
                materialized.ok().and_then(|m| {
                    if let GeometryArrayStorage::Lines {
                        coords, offsets, ..
                    } = m.storage()
                    {
                        match method {
                            SimplifyMethod::Vw => {
                                m.simplify_vw_packed_lines(coords, offsets, tolerance).ok()
                            },
                            SimplifyMethod::Dp => {
                                m.simplify_dp_packed_lines(coords, offsets, tolerance).ok()
                            },
                        }
                    } else {
                        None
                    }
                })
            } else {
                match method {
                    SimplifyMethod::Vw => self
                        .simplify_vw_packed_lines(coords, offsets, tolerance)
                        .ok(),
                    SimplifyMethod::Dp => self
                        .simplify_dp_packed_lines(coords, offsets, tolerance)
                        .ok(),
                }
            };
            return result.map(Ok);
        }
        None
    }
}
