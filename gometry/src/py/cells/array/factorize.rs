use super::*;

impl PyCellArray {
    /// Unique ids + per-unique counts (value_counts pass).
    fn value_count_entries(&self) -> std::result::Result<(Vec<u64>, Vec<i64>), FactorizationError> {
        let mut slot_of: HashMap<u64, usize> = HashMap::with_capacity(self.len());
        let mut uniques = Vec::with_capacity(self.len());
        let mut counts = Vec::<i64>::with_capacity(self.len());
        for row in 0..self.len() {
            if self.is_row_missing(row) {
                continue;
            }
            let id = self.id_at(row);
            crate::py::cells::record_first_seen_count!(slot_of, uniques, counts, id);
        }
        Ok(sort_value_counts(&uniques, &counts))
    }

    fn factorize_identity_entries(
        &self,
    ) -> std::result::Result<(Vec<u64>, Vec<i64>), FactorizationError> {
        let mut slot_of: HashMap<u64, usize> = HashMap::with_capacity(self.len());
        let mut uniques = Vec::with_capacity(self.len());
        let mut codes = Vec::with_capacity(self.len());
        macro_rules! append_code {
            ($id:ident) => {{
                let slot = crate::py::cells::first_seen_slot!(slot_of, uniques, $id);
                let Ok(code) = i64::try_from(slot) else {
                    return Err(FactorizationError::CodeOverflow);
                };
                codes.push(code);
            }};
        }
        let ids = self.storage.ids.as_slice();
        match self.storage.missing.as_ref() {
            None => {
                for id in ids.iter().copied() {
                    append_code!(id);
                }
            },
            Some(missing) => {
                for (id, is_missing) in ids.iter().copied().zip(missing.iter()) {
                    if is_missing {
                        codes.push(-1);
                    } else {
                        append_code!(id);
                    }
                }
            },
        }
        Ok((uniques, codes))
    }

    /// Unique ids + dense codes (factorize pass).
    fn factorize_entries(&self) -> std::result::Result<(Vec<u64>, Vec<i64>), FactorizationError> {
        if matches!(self.selection_ref(), RowSelectionRef::Identity) {
            return self.factorize_identity_entries();
        }
        let mut slot_of: HashMap<u64, usize> = HashMap::with_capacity(self.len());
        let mut uniques = Vec::with_capacity(self.len());
        let mut codes = Vec::with_capacity(self.len());
        for row in 0..self.len() {
            if self.is_row_missing(row) {
                codes.push(-1);
                continue;
            }
            let id = self.id_at(row);
            let slot = crate::py::cells::first_seen_slot!(slot_of, uniques, id);
            let Ok(code) = i64::try_from(slot) else {
                return Err(FactorizationError::CodeOverflow);
            };
            codes.push(code);
        }
        Ok((uniques, codes))
    }
}

#[pyo3::pymethods]
impl PyCellArray {
    /// Whether a cell id / cell object appears in the array.
    ///
    /// Returns
    /// -------
    /// bool
    fn __contains__(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> PyResult<bool> {
        if item.is_none() {
            let array = self.clone();
            return Ok(array.has_missing(py));
        }
        let kind = self.kind();
        let Some(needle) = membership_id(item, |item| kind.id_from_value(item))? else {
            return Ok(false);
        };
        let array = self.clone();
        Ok(py.detach(move || {
            (0..array.len()).any(|row| !array.is_row_missing(row) && array.id_at(row) == needle)
        }))
    }

    /// Number of elements with the same cell id.
    ///
    /// Parameters
    /// ----------
    /// value : cell object or int id
    ///     The element to count.
    ///
    /// Returns
    /// -------
    /// int
    fn count(&self, value: &Bound<'_, PyAny>) -> PyResult<usize> {
        if value.is_none() {
            if self.storage.missing.is_none() {
                return Ok(0);
            }
            return Ok(value.py().detach(|| {
                (0..self.len())
                    .filter(|&row| self.is_row_missing(row))
                    .count()
            }));
        }
        let kind = self.kind();
        let Some(needle) = membership_id(value, |value| kind.id_from_value(value))? else {
            return Ok(0);
        };
        Ok(value.py().detach(|| {
            (0..self.len())
                .filter(|&row| !self.is_row_missing(row) && self.id_at(row) == needle)
                .count()
        }))
    }

    /// Factorize cells into dense integer codes and first-seen uniques
    /// (pandas factorize parity).
    ///
    /// Returns
    /// -------
    /// tuple
    ///     ``(codes, unique_cells)`` where ``codes`` is a read-only `int64`
    ///     ndarray and ``unique_cells`` is a CellArray.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> neighbor = list(cell.neighbors)[0]
    /// >>> assert neighbor is not None
    /// >>> codes, unique = gm.CellArray([cell, neighbor]).factorize()
    /// >>> codes.tolist()
    /// [0, 1]
    fn factorize(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (uniques, codes) = match py.detach(|| {
            self.factorize_entries()
                .map(|(uniques, codes)| (Self::from_ids(self.kind(), uniques), codes))
        }) {
            Ok(values) => values,
            Err(error) => {
                return Err(error.into_py_err(
                    "cell value count overflows int64",
                    "cell factor code overflows int64",
                ));
            },
        };
        let codes = match int64_array(py, codes) {
            Ok(codes) => codes,
            Err(error) => {
                drop(uniques);
                return Err(error);
            },
        };
        let cells = match uniques.into_pyobject(py) {
            Ok(cells) => cells.into_any().unbind(),
            Err(error) => {
                drop(codes);
                return Err(error);
            },
        };
        Ok(PyTuple::new(py, [codes, cells])?.into_any().unbind())
    }

    /// First index of an equal cell id in `[start, stop)`.
    ///
    /// Parameters
    /// ----------
    /// value : cell object or int id
    ///     The element to locate.
    /// start : int, default 0
    ///     First position searched.
    /// stop : int, optional
    ///     One past the last position searched (the array length when
    ///     omitted).
    ///
    /// Returns
    /// -------
    /// int
    ///     The first matching position.
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If no element in the window equals ``value``.
    #[pyo3(signature = (value, start = 0, stop = None), text_signature = "($self, value, start=0, stop=None)")]
    fn index(
        &self,
        value: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = crate::strict_i64)] start: i64,
        #[pyo3(from_py_with = crate::strict_optional_i64)] stop: Option<i64>,
    ) -> PyResult<usize> {
        let needle = if value.is_none() {
            None
        } else {
            let kind = self.kind();
            let Some(needle) = membership_id(value, |value| kind.id_from_value(value))? else {
                let value = value
                    .repr()
                    .and_then(|repr| repr.extract::<String>())
                    .unwrap_or_else(|_| "value".to_owned());
                return Err(PyValueError::new_err(format!("{value} is not in array")));
            };
            Some(needle)
        };
        let len = self.len();
        let clamp = |bound: i64| -> usize {
            let resolved = if bound < 0 {
                bound
                    .checked_add(i64::try_from(len).unwrap_or(i64::MAX))
                    .expect("negative index adjustment fits i64")
            } else {
                bound
            };
            usize::try_from(resolved.max(0)).unwrap_or(0).min(len)
        };
        let start = clamp(start);
        let stop = stop.map_or(len, clamp);
        if start < stop
            && (needle.is_some() || self.storage.missing.is_some())
            && let Some(row) = value.py().detach(|| {
                (start..stop).find(|&row| {
                    needle.is_none() == self.is_row_missing(row)
                        && needle.is_none_or(|needle| self.id_at(row) == needle)
                })
            })
        {
            return Ok(row);
        }
        let value = value
            .repr()
            .and_then(|repr| repr.extract::<String>())
            .unwrap_or_else(|_| "cell".to_owned());
        Err(PyValueError::new_err(format!("{value} is not in array")))
    }

    /// Unique cells and counts, ordered by descending count (pandas
    /// value_counts parity), with first appearance breaking ties.
    ///
    /// Returns
    /// -------
    /// tuple
    ///     ``(unique_cells, counts)`` where ``unique_cells`` is a CellArray
    ///     and ``counts`` is a read-only `int64` ndarray.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> p = gm.Point(-122.4194, 37.7749, crs=4326)
    /// >>> cell = gm.h3_cover(p, resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> unique, counts = gm.CellArray([cell, cell]).value_counts()
    /// >>> counts.tolist()
    /// [2]
    fn value_counts(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (sorted_uniques, sorted_counts) = match py.detach(|| {
            self.value_count_entries()
                .map(|(uniques, counts)| (Self::from_ids(self.kind(), uniques), counts))
        }) {
            Ok(values) => values,
            Err(error) => {
                return Err(error.into_py_err(
                    "cell value count overflows int64",
                    "cell factor code overflows int64",
                ));
            },
        };
        let cells = match sorted_uniques.into_pyobject(py) {
            Ok(cells) => cells.into_any().unbind(),
            Err(error) => {
                drop(sorted_counts);
                return Err(error);
            },
        };
        let counts = match int64_array(py, sorted_counts) {
            Ok(counts) => counts,
            Err(error) => {
                drop(cells);
                return Err(error);
            },
        };
        Ok(PyTuple::new(py, [cells, counts])?.into_any().unbind())
    }
}
