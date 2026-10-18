use super::*;

frozen_pymethods! {
impl PyCellArray {
    // NEP 13: opt out of numpy ufunc dispatch — cell ids are opaque keys, not
    // numbers to broadcast over.
    #[classattr]
    #[expect(non_upper_case_globals, reason = "Python dunder name")]
    const __array_ufunc__: Option<Py<PyAny>> = None;

    /// Construct a cell array from typed cells, ids, or tokens.
    ///
    /// The grid is inferred from a typed cell when ``type=None``. H3, S2, and
    /// Tile ids and any grid's tokens may accompany a typed cell from the same
    /// grid; Geohash cells accept tokens, not integer ids. Supply ``type=`` for
    /// raw-only values, numeric arrays or buffers, empty input, or input
    /// containing only ``None``. Wrap a single cell or supported id in an
    /// iterable; a bare token string is ambiguous, so wrap one token in a
    /// one-item iterable.
    ///
    /// Parameters
    /// ----------
    /// values : iterable, numpy.ndarray, or buffer
    ///     Typed cells and ``None``; raw ids for H3/S2/Tile and string tokens
    ///     are accepted when the grid can be inferred. Iterables are consumed
    ///     in input order.
    /// type : type
    ///     One of the native ``H3Cell``, ``S2Cell``, ``Tile``, or
    ///     ``GeohashCell`` classes, required when the grid cannot be inferred.
    ///
    /// Returns
    /// -------
    /// CellArray
    ///     An immutable array in input order; ``None`` entries remain missing
    ///     rows.
    ///
    /// Raises
    /// ------
    /// TypeError
    ///     If the grid cannot be inferred, ``type`` is unsupported, or a value
    ///     does not match the selected grid.
    /// ParseError
    ///     If an id or token is invalid for the selected grid.
    #[new]
    #[pyo3(signature = (values, *, r#type=None))]
    fn new(
        values: &Bound<'_, PyAny>,
        r#type: Option<&Bound<'_, PyType>>,
    ) -> PyResult<Self> {
        let (kind, collected) = if let Some(cell_type) = r#type {
            let kind = grid_kind_from_type(cell_type)?;
            (kind, collect_ids(values, kind)?)
        } else {
            collect_inferred_ids(values)?
        };
        let storage = CellStorage::new(kind, collected.ids, collected.missing);
        Ok(Self {
            storage,
            selection: RowSelection::Identity,
        })
    }

    /// ``sys.getsizeof`` support: Python's allocation base plus this array's logical
    /// ``uint64`` id payload, missing flags, and any row-selection map. Shared
    /// backing buffers are estimated from the selected view, not the full
    /// parent allocation.
    ///
    /// Raises
    /// ------
    /// OverflowError
    ///     If the reported size exceeds Python's ``Py_ssize_t`` range.
    fn __sizeof__(slf: &Bound<'_, Self>) -> PyResult<isize> {
        python_sizeof(slf.py(), slf.as_any(), slf.borrow().heap_bytes())
    }

    /// Grid-system token for the stored cell type.
    ///
    /// Returns
    /// -------
    /// str
    #[getter]
    const fn grid(&self) -> &'static str {
        self.kind().token()
    }

    /// Logical id payload in bytes (`len * 8`).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the u64 id column length is bounded by Vec layout, so its logical byte length fits usize"
    )]
    fn nbytes(&self) -> usize {
        self.len() * size_of::<u64>()
    }

    /// Number of cells.
    ///
    /// Returns
    /// -------
    /// int
    fn __len__(&self) -> usize {
        self.len()
    }

    /// ``False`` only when the array is empty.
    ///
    /// Returns
    /// -------
    /// bool
    fn __bool__(&self) -> bool {
        self.len() != 0
    }

    /// Select cells by integer-like scalar, slice, or fancy index.
    ///
    /// An ``int``, object implementing ``__index__``, or zero-dimensional
    /// integer ndarray returns one cell object or ``None`` for a missing row.
    /// A ``slice``, integer sequence, one-dimensional integer ndarray, or
    /// boolean mask returns a ``CellArray`` of the same cell kind. Boolean
    /// scalars and NumPy index arrays with rank above one raise ``TypeError``.
    ///
    /// Returns
    /// -------
    /// Cell, None, or CellArray
    fn __getitem__(&self, py: Python<'_>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        array_getitem(self, py, index)
    }

    /// Iterate cells in row order.
    ///
    /// Returns
    /// -------
    /// iterator of Cell
    fn __iter__(&self) -> PyCellArrayIter {
        PyCellArrayIter::new(self.clone(), false)
    }

    /// Iterate cells in reverse row order.
    ///
    /// Returns
    /// -------
    /// iterator of Cell
    fn __reversed__(&self) -> PyCellArrayIter {
        PyCellArrayIter::new(self.clone(), true)
    }

    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> Py<PyAny> {
        let Ok(other) = other.extract::<PyRef<Self>>() else {
            return py.NotImplemented();
        };
        let this = self;
        let other = &*other;
        crate::broadcast::py_bool(py, py.detach(|| this.logical_eq(other)))
    }

    fn __hash__(&self, py: Python<'_>) -> u64 {
        use std::hash::{Hash as _, Hasher as _};
        py.detach(|| {
            let mut hasher = crate::collections::python_hasher();
            self.kind().token().hash(&mut hasher);
            self.len().hash(&mut hasher);
            for row in 0..self.len() {
                let missing = self.is_row_missing(row);
                missing.hash(&mut hasher);
                if !missing {
                    self.id_at(row).hash(&mut hasher);
                }
            }
            hasher.finish()
        })
    }

    /// Per-row missing mask as a read-only boolean NumPy array.
    ///
    /// Bulk cell factories set a missing row when the corresponding geometry
    /// was missing; dense constructions return all-``False``.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray of bool
    #[getter]
    fn is_missing<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, numpy::PyArray1<bool>>> {
        if self.storage.missing.is_none() {
            return Ok(false_bool_array(py, self.len())?
                .into_bound(py)
                .cast_into()?);
        }
        let missing = py.detach(|| {
            (0..self.len())
                .map(|row| self.is_row_missing(row))
                .collect()
        });
        Ok(bool_array(py, missing)?.into_bound(py).cast_into()?)
    }

    pub(crate) fn __repr__(&self) -> String {
        format!("<CellArray[{}] len={}>", self.kind().class_name(), self.len())
    }
}
}
