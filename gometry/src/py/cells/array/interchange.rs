use pyo3::IntoPyObjectExt as _;

use super::*;

impl PyCellArray {
    fn arrow_id_buffer(&self) -> (crate::geometry::SharedSlice<u64>, usize) {
        self.selection_ref().csr_rows(0..self.len()).map_or_else(
            || {
                (
                    crate::geometry::SharedSlice::from_vec(self.logical_ids_vec()),
                    0,
                )
            },
            |rows| (self.storage.ids.clone(), rows.start),
        )
    }

    fn arrow_missing_mask(&self) -> Option<ValidityMask> {
        match self.selection_ref() {
            RowSelectionRef::Identity => self.storage.missing.clone(),
            RowSelectionRef::Window { start, len } => {
                self.storage.missing.as_ref().map(|missing| {
                    missing
                        .window(start, len)
                        .expect("selected rows are admitted")
                })
            },
            RowSelectionRef::Gather(_) => self.missing_mask_and_present_count().0,
        }
    }

    fn geohash_arrow_buffers(
        &self,
    ) -> PyResult<(
        crate::geometry::SharedSlice<i32>,
        crate::geometry::SharedSlice<u8>,
    )> {
        let mut total_bytes = 0_usize;
        for row in 0..self.len() {
            if self.is_row_missing(row) {
                continue;
            }
            let cell = Geohash::from_validated_id(self.id_at(row));
            let (_, length) = cell.token_bytes();
            total_bytes = total_bytes.checked_add(length).ok_or_else(|| {
                PyOverflowError::new_err("geohash tokens exceed Arrow UTF-8 size")
            })?;
        }
        if i32::try_from(total_bytes).is_err() {
            return Err(PyOverflowError::new_err(
                "geohash tokens exceed Arrow UTF-8 offset range",
            ));
        }
        let offset_count = self.len().checked_add(1).ok_or_else(|| {
            PyOverflowError::new_err("geohash row count exceeds Arrow offset capacity")
        })?;
        let mut offsets = Vec::with_capacity(offset_count);
        let mut values = Vec::with_capacity(total_bytes);
        offsets.push(0_i32);
        for row in 0..self.len() {
            if !self.is_row_missing(row) {
                let cell = Geohash::from_validated_id(self.id_at(row));
                let (bytes, length) = cell.token_bytes();
                values.extend_from_slice(
                    bytes
                        .get(..length)
                        .expect("geohash token length fits its buffer"),
                );
            }
            offsets.push(i32::try_from(values.len()).expect("reserved Arrow UTF-8 offset range"));
        }
        Ok((
            crate::geometry::SharedSlice::from_vec(offsets),
            crate::geometry::SharedSlice::from_vec(values),
        ))
    }

    fn tokens<G: CellArrayGrid>(&self) -> Vec<Option<String>> {
        let mut tokens = Vec::with_capacity(self.len());
        for row in 0..self.len() {
            let token = if self.is_row_missing(row) {
                None
            } else {
                Some(G::from_validated_id(self.id_at(row)).token())
            };
            tokens.push(token);
        }
        tokens
    }

    fn geohash_tokens(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let tokens = PyList::empty(py);
        for row in 0..self.len() {
            if self.is_row_missing(row) {
                tokens.append(py.None())?;
                continue;
            }
            let cell = Geohash::from_validated_id(self.id_at(row));
            let (bytes, length) = cell.token_bytes();
            let token = std::str::from_utf8(
                bytes
                    .get(..length)
                    .expect("geohash token length fits its buffer"),
            )
            .expect("geohash alphabet is ASCII");
            tokens.append(PyString::new(py, token))?;
        }
        Ok(tokens.into_any().unbind())
    }
}

pub(super) enum CellNumpyArrayKind {
    Ids {
        dtype: Option<Py<PyAny>>,
        native: bool,
    },
    Objects,
}

#[pyo3::pymethods]
impl PyCellArray {
    /// NumPy array protocol.
    ///
    /// With `dtype=None`, H3/S2/tile arrays export raw uint64 ids and
    /// Geohash arrays export typed GeohashCell objects. `dtype=uint64` is
    /// available only for the numeric grids; `dtype=object` exports typed
    /// cell objects for every grid, matching iteration.
    ///
    /// Parameters
    /// ----------
    /// dtype : uint64 or object, optional
    /// copy : bool, optional
    ///     ``False`` requires a contiguous zero-copy numeric-id export;
    ///     opposite-endian numeric ids, gathered ids, and every object export
    ///     raise because they materialize.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    #[pyo3(signature = (dtype=None, copy=None))]
    fn __array__(
        slf: Bound<'_, Self>,
        dtype: Option<&Bound<'_, PyAny>>,
        copy: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let kind = slf.borrow().numpy_array_kind(py, dtype)?;
        match kind {
            CellNumpyArrayKind::Ids { dtype, native } => {
                if !native && copy == Some(false) {
                    return Err(GeometryError::new_err(
                        "gometry cannot return the requested array without copying",
                    ));
                }
                if copy == Some(false) {
                    let can_borrow = slf.borrow().logical_contiguous_ids().is_some();
                    if !can_borrow {
                        return Err(GeometryError::new_err(
                            "gometry cannot return the requested array without copying",
                        ));
                    }
                }
                let array = if copy == Some(true) {
                    let ids = slf.borrow().logical_ids_vec();
                    uint64_array(py, ids)?
                } else {
                    Self::to_numpy(slf)?
                };
                if !native {
                    let dtype = dtype.expect("non-native ids require a requested dtype");
                    return astype_readonly(array.bind(py), dtype.bind(py));
                }
                Ok(array)
            },
            CellNumpyArrayKind::Objects => {
                if copy == Some(false) {
                    return Err(GeometryError::new_err(
                        "gometry cannot return the requested array without copying",
                    ));
                }
                slf.borrow().object_numpy_array(py)
            },
        }
    }

    /// Export cell identities through the Arrow C Data Interface.
    ///
    /// H3, S2, and tile arrays use nullable ``uint64`` IDs. Geohash arrays use
    /// nullable UTF-8 tokens. Numeric columns do not retain grid kind; restore
    /// it with ``CellArray(..., type=...)`` when importing raw IDs.
    ///
    /// Parameters
    /// ----------
    /// requested_schema : capsule or None, default None
    ///     A matching Arrow C schema capsule. A different schema is rejected.
    ///
    /// Returns
    /// -------
    /// tuple of capsule
    ///     The Arrow schema and array capsules. Their owners retain exported
    ///     buffers until the consumer releases the array.
    ///
    /// Raises
    /// ------
    /// TypeError
    ///     If ``requested_schema`` does not match the exported value type.
    /// OverflowError
    ///     If the encoded geohash tokens exceed Arrow UTF-8's 32-bit offset range.
    #[pyo3(signature = (requested_schema=None, /))]
    fn __arrow_c_array__(
        &self,
        py: Python<'_>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        if self.kind() == GridKind::GeohashCell {
            let (offsets, values) = self.geohash_arrow_buffers()?;
            let mut array = arrow_c::utf8_array(offsets, values)?;
            if let Some(missing) = self.arrow_missing_mask() {
                arrow_c::apply_top_level_validity(&mut array, &missing)?;
            }
            return arrow_c::array_capsules_with_schema(
                py,
                arrow_c::utf8_schema(),
                array,
                requested_schema,
            );
        }

        let length = self.len();
        let (ids, start) = self.arrow_id_buffer();
        let mut array = arrow_c::primitive_u64_array_windowed(ids, start, length)?;
        if let Some(missing) = self.arrow_missing_mask() {
            arrow_c::apply_top_level_validity(&mut array, &missing)?;
        }
        arrow_c::array_capsules_with_schema(
            py,
            arrow_c::primitive_u64_schema(true),
            array,
            requested_schema,
        )
    }

    #[expect(
        clippy::type_complexity,
        reason = "pickle tuple matches CPython reduce contract"
    )]
    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, (Py<PyAny>, String, Py<PyAny>))> {
        let callable = crate::gometry_lib_module(py)?
            .getattr(pyo3::intern!(py, "_unpickle_cell_array_v1"))?
            .unbind();
        let payload = if self.kind() == GridKind::GeohashCell {
            let tokens = dispatch_cell_grid!(self, G, {
                let mut tokens = Vec::with_capacity(self.len());
                for row in 0..self.len() {
                    if !self.is_row_missing(row) {
                        tokens.push(G::from_validated_id(self.id_at(row)).token());
                    }
                }
                tokens
            });
            tokens.into_py_any(py)?
        } else {
            let mut ids = Vec::with_capacity(self.len());
            for row in 0..self.len() {
                if !self.is_row_missing(row) {
                    ids.push(self.id_at(row));
                }
            }
            ids.into_py_any(py)?
        };
        let mask: Py<PyAny> = if (0..self.len()).any(|row| self.is_row_missing(row)) {
            let bytes: Vec<_> = (0..self.len())
                .map(|row| u8::from(self.is_row_missing(row)))
                .collect();
            PyBytes::new(py, &bytes).into_any().unbind()
        } else {
            py.None()
        };
        Ok((callable, (payload, self.kind().token().to_owned(), mask)))
    }

    /// Return a read-only NumPy identity column.
    ///
    /// H3, S2, and tile arrays expose their validated ids as uint64
    /// (zero-copy for contiguous selections). Geohash has no public numeric
    /// id, so it returns an object array of typed GeohashCell values.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> type(gm.CellArray([cell]).to_numpy()).__name__
    /// 'ndarray'
    fn to_numpy(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let owner = slf.clone().into_any();
        let borrowed = slf.borrow();
        if borrowed.kind() == GridKind::GeohashCell
            || (0..borrowed.len()).any(|row| borrowed.is_row_missing(row))
        {
            return borrowed.object_numpy_array(owner.py());
        }
        if let Some(values) = borrowed.logical_contiguous_ids() {
            // SAFETY: values is tied to slf, and owner pins that object as
            // the array base.
            return unsafe { uint64_slice_array(owner, values) };
        }
        uint64_array(owner.py(), borrowed.logical_ids_vec())
    }

    /// Canonical string token of every cell, in order.
    ///
    /// For H3, S2, and tiles this is the text form of the numeric id exposed
    /// by `to_numpy()`. For Geohash it is the public string identity itself;
    /// Geohash `to_numpy()` instead returns typed `GeohashCell` objects.
    ///
    /// Returns
    /// -------
    /// list of str or None
    ///     One canonical token per logical row (H3 hex, S2 token, geohash, or
    ///     tile quadkey); ``None`` marks a missing row.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CellArray([gm.GeohashCell('u33d')]).token
    /// ['u33d']
    #[getter]
    pub(crate) fn token(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        use pyo3::IntoPyObjectExt as _;
        let tokens = match self.kind() {
            GridKind::H3Cell => self.tokens::<h3o::CellIndex>(),
            GridKind::S2Cell => self.tokens::<CellId>(),
            GridKind::Tile => self.tokens::<Tile>(),
            GridKind::GeohashCell => return self.geohash_tokens(py),
        };
        tokens.into_py_any(py)
    }
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled CellArray from its public identities and logical mask.
#[pyfunction]
pub(crate) fn _unpickle_cell_array_v1(
    ids: &Bound<'_, PyAny>,
    grid: &str,
    mask: &Bound<'_, PyAny>,
) -> PyResult<PyCellArray> {
    let kind = GridKind::parse(grid)?;
    let (ids, missing) = if mask.is_none() {
        let collected = super::grid_kind::collect_ids(ids, kind)?;
        (collected.ids, collected.missing)
    } else {
        let mask = mask
            .cast::<PyBytes>()
            .map_err(|_| PyValueError::new_err("CellArray pickle mask must be bytes or None"))?;
        let bytes = mask.as_bytes();
        let mut present_count = 0_usize;
        for &value in bytes {
            match value {
                0 => present_count = present_count.wrapping_add(1),
                1 => {},
                _ => {
                    return Err(PyValueError::new_err(
                        "CellArray pickle mask must contain only 0 or 1",
                    ));
                },
            }
        }
        let present = super::grid_kind::collect_ids(ids, kind)?;
        if present.missing.is_some() {
            return Err(PyValueError::new_err(
                "CellArray pickle identities must not contain missing rows",
            ));
        }
        let present = present.ids;
        if present_count != present.len() {
            return Err(PyValueError::new_err(
                "CellArray pickle mask present count does not match identities",
            ));
        }
        let mut logical_ids = Vec::with_capacity(bytes.len());
        let mut present = present.into_iter();
        for &value in bytes {
            logical_ids.push(if value == 0 {
                present
                    .next()
                    .expect("mask present count matches identities")
            } else {
                0
            });
        }
        let missing = storage::ValidityMask::from_missing_iter(
            bytes.len(),
            bytes.iter().map(|&value| value == 1),
        );
        (logical_ids, missing)
    };
    Ok(PyCellArray {
        storage: CellStorage::new(kind, ids, missing),
        selection: RowSelection::Identity,
    })
}
