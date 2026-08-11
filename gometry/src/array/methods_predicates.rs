#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use numpy::PyArrayMethods as _;
use pyo3::IntoPyObjectExt as _;

use crate::array::{
    ArrowEncoding, Bound, F64Param, GeometryArrayStorage, GeometryError, Py, PyAny,
    PyAnyMethods as _, PyErr, PyGeometry, PyGeometryArray, PyResult, PyValueError, Python,
    RowSelectionRef, SimplifyMethod, Typed, packed_points_to_arrow, pymethods,
};

impl PyGeometryArray {
    pub(crate) fn materialized_objects(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        use pyo3::IntoPyObjectExt as _;
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
        // Packed storage has one frame by construction. Avoiding `items()` is
        // load-bearing: on packed arrays it would synthesize one `PyGeometry`
        // per row before WKB export.
        return crate::py::arrow::storage_to_wkb_arrow(
            py,
            array.storage(),
            array.crs_str(),
            array.epoch(),
        );
    }
    // Gather materializes once via the shared memo; Identity/Window share
    // parent coordinate Arcs (Window via `CoordSeq::view` / rebased CSR).
    let storage =
        crate::array::normalized_gather_storage(array.storage_arc(), &array.gathered_memo)
            .map_err(crate::array::packed_columns_err)?;
    match storage.as_ref() {
        GeometryArrayStorage::Points { coords, row_map } => {
            let seq = match row_map.as_deref() {
                RowSelectionRef::Identity | RowSelectionRef::Gather(_) => coords.as_ref().clone(),
                RowSelectionRef::Window { start, len } => coords.view(
                    crate::geometry::CoordWindow::trusted(start..start + len, coords.len()),
                ),
            };
            packed_points_to_arrow(py, &seq, array.crs_str(), array.epoch())
        },
        GeometryArrayStorage::Lines {
            coords,
            offsets,
            row_map,
        } => export_packed_lines_selection(
            py,
            coords,
            offsets,
            row_map.as_deref(),
            array.crs_str(),
            array.epoch(),
        ),
        GeometryArrayStorage::Polygons {
            coords,
            ring_offsets,
            polygon_offsets,
            row_map,
        } => export_packed_polygons_selection(
            py,
            coords,
            ring_offsets,
            polygon_offsets,
            row_map.as_deref(),
            array.crs_str(),
            array.epoch(),
        ),
        GeometryArrayStorage::Mixed(shapes) => {
            crate::py::arrow::shapes_to_arrow(py, shapes, array.crs_str(), array.epoch())
        },
    }
}

/// PyArrow packed-line export for Identity (full CSR share) or Window
/// (rebased offsets + windowed coords that still share parent column Arcs).
fn export_packed_lines_selection(
    py: Python<'_>,
    coords: &crate::geometry::CoordSeq,
    offsets: &crate::geometry::CsrOffsetColumn,
    row_map: RowSelectionRef<'_>,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    match row_map {
        RowSelectionRef::Identity | RowSelectionRef::Gather(_) => {
            crate::py::arrow::packed_lines_to_arrow(py, coords, offsets, crs, epoch)
        },
        RowSelectionRef::Window { start, len } => {
            let offset_slice = offsets.as_slice();
            let vertex_start = offset_slice[start] as usize;
            let vertex_end = offset_slice[start + len] as usize;
            let viewed = coords.view(crate::geometry::CoordWindow::trusted(
                vertex_start..vertex_end,
                coords.len(),
            ));
            let rebased = rebase_offsets_window(offset_slice, start, len)?;
            crate::py::arrow::packed_lines_to_arrow(py, &viewed, &rebased, crs, epoch)
        },
    }
}

fn export_packed_polygons_selection(
    py: Python<'_>,
    coords: &crate::geometry::CoordSeq,
    ring_offsets: &crate::geometry::CsrOffsetColumn<crate::geometry::RingLevel>,
    polygon_offsets: &crate::geometry::CsrOffsetColumn<crate::geometry::PolygonLevel>,
    row_map: RowSelectionRef<'_>,
    crs: Option<&str>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    match row_map {
        RowSelectionRef::Identity | RowSelectionRef::Gather(_) => {
            crate::py::arrow::packed_polygons_to_arrow(
                py,
                coords,
                ring_offsets,
                polygon_offsets,
                crs,
                epoch,
            )
        },
        RowSelectionRef::Window { start, len } => {
            let ring_slice = ring_offsets.as_slice();
            let polygon_slice = polygon_offsets.as_slice();
            let ring_start = polygon_slice[start] as usize;
            let ring_end = polygon_slice[start + len] as usize;
            let vertex_start = ring_slice[ring_start] as usize;
            let vertex_end = ring_slice[ring_end] as usize;
            let viewed = coords.view(crate::geometry::CoordWindow::trusted(
                vertex_start..vertex_end,
                coords.len(),
            ));
            let (out_rings, out_polygons) =
                rebase_polygon_offsets_window(ring_slice, polygon_slice, start, len)?;
            crate::py::arrow::packed_polygons_to_arrow(
                py,
                &viewed,
                &out_rings,
                &out_polygons,
                crs,
                epoch,
            )
        },
    }
}

fn rebase_offsets_window(
    offsets: &[i32],
    start: usize,
    len: usize,
) -> PyResult<crate::geometry::CsrOffsetColumn> {
    let window = &offsets[start..=(start + len)];
    let base = window[0] as usize;
    let rebased: Vec<usize> = window
        .iter()
        .map(|&offset| {
            (offset as usize)
                .checked_sub(base)
                .ok_or_else(|| GeometryError::new_err("malformed CSR offsets for Arrow export"))
        })
        .collect::<PyResult<_>>()?;
    let vertex_cap = *rebased
        .last()
        .ok_or_else(|| GeometryError::new_err("malformed CSR offsets for Arrow export"))?;
    crate::geometry::CsrOffsetColumn::<()>::try_new(rebased, vertex_cap).map_err(PyErr::from)
}

fn rebase_polygon_offsets_window(
    ring_offsets: &[i32],
    polygon_offsets: &[i32],
    start: usize,
    len: usize,
) -> PyResult<(
    crate::geometry::CsrOffsetColumn<crate::geometry::RingLevel>,
    crate::geometry::CsrOffsetColumn<crate::geometry::PolygonLevel>,
)> {
    let ring_start = polygon_offsets[start] as usize;
    let ring_end = polygon_offsets[start + len] as usize;
    let vertex_base = ring_offsets[ring_start] as usize;
    let rebased_rings: Vec<usize> = ring_offsets[ring_start..=ring_end]
        .iter()
        .map(|&offset| {
            (offset as usize)
                .checked_sub(vertex_base)
                .ok_or_else(|| GeometryError::new_err("malformed CSR offsets for Arrow export"))
        })
        .collect::<PyResult<_>>()?;
    let ring_cap = *rebased_rings
        .last()
        .ok_or_else(|| GeometryError::new_err("malformed CSR offsets for Arrow export"))?;
    let out_ring_offsets = crate::geometry::CsrOffsetColumn::<crate::geometry::RingLevel>::try_new(
        rebased_rings,
        ring_cap,
    )
    .map_err(PyErr::from)?;
    let polygon_base = ring_start;
    let rebased_polygons: Vec<usize> = polygon_offsets[start..=start + len]
        .iter()
        .map(|&offset| {
            (offset as usize)
                .checked_sub(polygon_base)
                .ok_or_else(|| GeometryError::new_err("malformed CSR offsets for Arrow export"))
        })
        .collect::<PyResult<_>>()?;
    let polygon_cap = *rebased_polygons
        .last()
        .ok_or_else(|| GeometryError::new_err("malformed CSR offsets for Arrow export"))?;
    let out_polygon_offsets =
        crate::geometry::CsrOffsetColumn::<crate::geometry::PolygonLevel>::try_new(
            rebased_polygons,
            polygon_cap,
        )
        .map_err(PyErr::from)?;
    Ok((out_ring_offsets, out_polygon_offsets))
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
                let geometry = Typed(self.geometry_at(index));
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
                if let GeometryArrayStorage::Lines {
                    coords, offsets, ..
                } = materialized.storage()
                {
                    return Some(match method {
                        SimplifyMethod::Vw => {
                            materialized.simplify_vw_packed_lines(coords, offsets, tolerance)
                        },
                        SimplifyMethod::Dp => {
                            materialized.simplify_dp_packed_lines(coords, offsets, tolerance)
                        },
                    });
                }
                return None;
            }
            return Some(match method {
                SimplifyMethod::Vw => self.simplify_vw_packed_lines(coords, offsets, tolerance),
                SimplifyMethod::Dp => self.simplify_dp_packed_lines(coords, offsets, tolerance),
            });
        }
        None
    }
}
