use std::fmt::Write as _;

use crate::py::classes::geometry_methods::{Bound, Py, PyAny, PyErr, PyGeometry, PyResult, Python};
use crate::py::functions::polyline;
use crate::py::{arrow, arrow_c, errors, functions};
use crate::{
    ArrowEncoding, HeapSize as _, OverlayOp, PyValueError, exact_geometry, io, overlay_operator,
    py_bool,
};

frozen_pymethods! {
impl PyGeometry {
    /// Encode the ``LineString`` or ``Point`` as Google polyline text (see
    /// `from_polyline`).
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
    /// str
    ///     The encoded polyline.
    ///
    /// Raises
    /// ------
    /// GeometryTypeError
    ///     If the geometry is not a ``LineString`` or ``Point``.
    /// CRSError
    ///     If the CRS is set and is not EPSG:4326 longitude/latitude.
    /// InvalidGeometryError
    ///     If the geometry carries Z/M, or a coordinate is outside the
    ///     longitude/latitude domain. Flatten explicitly with ``force_2d()``.
    /// GeometryError
    ///     If ``precision`` is out of range.
    ///
    /// See Also
    /// --------
    /// from_polyline : Decode Google polyline text into geometries.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.LineString([(-120.2, 38.5), (-120.95, 40.7)]).to_polyline()
    /// '_p~iF~ps|U_ulLnnqC'
    #[pyo3(signature = (*, precision = 5, drop_epoch = false))]
    pub fn to_polyline(
        &self,
        #[pyo3(from_py_with = crate::strict_i32)] precision: i32,
        drop_epoch: bool,
    ) -> PyResult<String> {
        errors::require_epoch_drop(
            self.epoch(),
            drop_epoch,
            "to_polyline",
        )?;
        let factor = polyline::polyline_precision_factor(precision)?;
        polyline::polyline_of(self, factor)
    }

    /// Export the geometry as a `GeoArrow` array.
    ///
    /// Parameters
    /// ----------
    /// encoding : {'auto', 'wkb'}, default auto
    ///     ``auto`` exports the geometry as its native GeoArrow layout;
    ///     ``wkb`` exports a GeoArrow WKB array.
    ///
    /// Returns
    /// -------
    /// object
        ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> type(gm.Point(1, 2).to_arrow()).__name__
    /// 'ExtensionArray'
    #[pyo3(signature = (*, encoding = ArrowEncoding::Auto), text_signature = "($self, *, encoding='auto')")]
pub fn to_arrow(&self, py: Python<'_>, encoding: ArrowEncoding) -> PyResult<Py<PyAny>> {
        match encoding {
            ArrowEncoding::Auto => arrow::shapes_to_arrow(
                py,
                std::slice::from_ref(self.shape.shape()),
                self.crs_str(),
                self.epoch(),
            ),
            ArrowEncoding::Wkb => arrow::shapes_to_wkb_arrow(
                py,
                std::slice::from_ref(self.shape.shape()),
                self.crs_str(),
                self.epoch(),
            ),
        }
    }

    pub fn __arrow_c_schema__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        arrow_c::geometry_to_schema_capsule(py, self)
    }

    #[pyo3(signature = (requested_schema = None))]
    pub fn __arrow_c_array__(
        &self,
        py: Python<'_>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        arrow::reject_requested_schema(requested_schema)?;
        arrow_c::geometry_to_array_capsules(py, self)
    }

    #[pyo3(signature = (requested_schema = None))]
    pub fn __arrow_c_stream__(
        &self,
        py: Python<'_>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        arrow::reject_requested_schema(requested_schema)?;
        arrow_c::geometry_to_stream_capsule(py, self)
    }
    pub fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        const WKT_LIMIT: usize = 120;
        // Render only the leading WKT bytes the preview needs — a 20k-vertex
        // geometry no longer materializes its full (~megabyte) WKT just to
        // truncate it. The bounded prefix is byte-identical to the full
        // render's, so the truncation result is unchanged.
        let shape = std::sync::Arc::clone(&self.shape);
        let mut wkt = py.detach(move || io::to_wkt_preview(shape.shape(), WKT_LIMIT + 1))?;
        if wkt.len() > WKT_LIMIT {
            wkt.truncate(WKT_LIMIT);
            wkt.push_str("...");
        }
        let mut out = format!("<{wkt}");
        if let Some(crs) = self.crs_str() {
            out.push(' ');
            out.push_str(crs);
        }
        if let Some(epoch) = self.epoch() {
            let _ = write!(out, " @{epoch}");
        }
        out.push('>');
        Ok(out)
    }

    pub fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        functions::reduce_geometry(py, self)
    }

    pub fn __and__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        overlay_operator(py, slf.as_any(), other, OverlayOp::Intersection)
    }

    pub fn __or__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        overlay_operator(py, slf.as_any(), other, OverlayOp::Union)
    }

    pub fn __sub__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        overlay_operator(py, slf.as_any(), other, OverlayOp::Difference)
    }

    pub fn __xor__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        overlay_operator(py, slf.as_any(), other, OverlayOp::SymmetricDifference)
    }

    pub fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        let shape = std::sync::Arc::clone(&self.shape);
        py.detach(move || io::to_wkt(shape.shape()))
            .map_err(PyErr::from)
    }

    /// ``False`` when the geometry is empty; ``True`` otherwise.
    ///
    /// Empty means a typed empty shape (for example ``POINT EMPTY``), not
    /// a missing array row.
    ///
    /// Returns
    /// -------
    /// bool
    pub fn __bool__(&self) -> bool {
        !self.shape.is_empty()
    }

    pub fn __format__(&self, py: Python<'_>, spec: &str) -> PyResult<String> {
        use std::fmt::Write as _;
        match spec {
            "" => {
                let shape = std::sync::Arc::clone(&self.shape);
                return py
                    .detach(move || io::to_wkt(shape.shape()))
                    .map_err(PyErr::from);
            },
            "x" | "X" => {
                let shape = std::sync::Arc::clone(&self.shape);
                let wkb = py.detach(move || io::to_wkb(shape.shape(), None, false))?;
                let Some(capacity) = wkb.len().checked_mul(2) else {
                    drop(wkb);
                    return Err(errors::native_overflow_error(
                        "hexadecimal WKB output size overflows usize",
                    ));
                };
                let mut out = String::new();
                if out.try_reserve(capacity).is_err() {
                    drop(wkb);
                    drop(out);
                    return Err(errors::native_memory_error());
                }
                for byte in wkb {
                    let _ = if spec == "x" {
                        write!(out, "{byte:02x}")
                    } else {
                        write!(out, "{byte:02X}")
                    };
                }
                return Ok(out);
            },
            _ => {},
        }
        let invalid = || PyValueError::new_err(format!("invalid format specifier: {spec}"));
        let body = spec.strip_prefix('0').unwrap_or(spec);
        let (rest, trim) = if let Some(rest) = body.strip_suffix(['f', 'F']) {
            (rest, false)
        } else if let Some(rest) = body.strip_suffix(['g', 'G']) {
            (rest, true)
        } else if body.ends_with(['x', 'X']) {
            return Err(PyValueError::new_err(
                "hex representation does not specify precision",
            ));
        } else if body.starts_with('.') {
            // Bare precision (`'.2'`) defaults the format code to 'g',
            // exactly like shapely.
            (body, true)
        } else {
            return Err(invalid());
        };
        let precision = match rest.strip_prefix('.') {
            Some(digits) => Some(digits.parse::<u8>().map_err(|_| invalid())?),
            None if rest.is_empty() => None,
            None => return Err(invalid()),
        };
        let format = match (trim, precision) {
            // Bare 'g' is the default shortest-trimmed rendering.
            (true, None) => io::WktNumberFormat::Shortest,
            (true, Some(precision)) => io::WktNumberFormat::Trimmed(precision),
            // Bare 'f' renders full fixed width, like shapely.
            (false, None) => io::WktNumberFormat::Fixed(16),
            (false, Some(precision)) => io::WktNumberFormat::Fixed(precision),
        };
        let shape = std::sync::Arc::clone(&self.shape);
        py.detach(move || io::to_wkt_display(shape.shape(), format))
            .map_err(PyErr::from)
    }

    /// Structural value equality: same CRS, coordinate epoch, geometry kind,
    /// and exact coordinates including Z/M axes and vertex order (matching
    /// `equals_identical`). Empty geometries compare by kind AND declared
    /// axes, so ``POINT Z EMPTY != POINT EMPTY``. Topological equivalence is
    /// the separate `equals` operation.
    pub fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> Py<PyAny> {
        exact_geometry(other).map_or_else(
            || py.NotImplemented(),
            |other| {
                py_bool(
                    py,
                    self.crs_ref() == other.crs_ref()
                        && self.epoch() == other.epoch()
                        && self.shape == other.shape,
                )
            },
        )
    }

    pub fn __hash__(&self) -> u64 {
        crate::collections::python_hash(&(
            self.crs_ref(),
            self.epoch().map(f64::to_bits),
            &self.shape,
        ))
    }
    /// Raw coordinate payload in bytes (numpy's ``nbytes`` convention): the
    /// stored ``f64`` ordinate columns only — object headers, prepared-state
    /// caches, and CRS metadata are excluded.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    pub fn nbytes(&self) -> usize {
        self.shape.shape().coordinate_bytes()
    }

    /// Retained native cost of this geometry for ``sys.getsizeof``.
    ///
    /// Counts the Python-facing struct, the Arc-owned ``ShapeData`` block
    /// (including the ``Shape`` payload — coordinate columns **and**
    /// container allocations such as multipart ``Vec``s, polygon hole
    /// ``Arc``s, and nested collection members), any attached
    /// *already-initialized* ``PreparedPlan`` products, and the Arc-owned
    /// frame-cache sidecar with any products already built on it. Spilled CRS
    /// frame text is included; inline CRS text is already part of the struct.
    /// Uninitialized lazy caches are not counted and this method never
    /// builds them — so two cold ``__sizeof__`` reads report the same
    /// size, and warming (``bounds``, ``prepare``, distance, …) can only
    /// increase it. Container geometries therefore scale with part/member
    /// count even when members carry no ordinate payload (e.g. empty
    /// points in a ``GeometryCollection``).
    ///
    /// ``nbytes`` remains the coordinate-only payload (numpy convention);
    /// use ``__sizeof__`` when measuring object retention.
    ///
    /// Returns
    /// -------
    /// int
    pub fn __sizeof__(&self) -> usize {
        self.total_size()
    }
}
}
