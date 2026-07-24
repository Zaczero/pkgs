#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Bulk result containers and internal PEP-3118 buffer holders.
//!
//! Public numeric lanes return read-only NumPy ndarrays; [`Groups`] is the
//! shared CSR ragged container (Arrow ListArray shape: flat `values` +
//! `offsets`). Spatial-index match call sites use the `Int64` backing; geometry
//! row groups use the `Geometry` backing; ragged cell rows (a cell array's
//! `neighbors`/`children`) use the `Cells` backing. Internal `_Float64Buffer`
//! / `_Int32Buffer` pyclasses move `Arc` column storage into pyarrow without a
//! second byte copy on little-endian hosts.

use std::borrow::Cow;
use std::ffi::c_int;
use std::ops::Range;
use std::sync::Arc;

use pyo3::exceptions::{PyBufferError, PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PySequence};
use pyo3::{IntoPyObjectExt, ffi};

use crate::HeapSize;
use crate::array::PyGeometryArray;
use crate::geometry::Shape;
use crate::py::buffer::{fill_typed_view, release_typed_view};
use crate::py::cells::{GridKind, PyCellArray};
use crate::py::errors::GeometryError;
use crate::py::numpy::{frozen_i64_view, int64_array};
use crate::py::row::{RowContainer, RowIndexOrSlice, RowIterState, parse_row_index_or_slice};

/// Format a short preview list of already-rendered row strings.
///
/// Callers own the per-row rendering so geometry/cell groups use the public
/// Python `__repr__` path rather than Rust `Debug`.
fn preview(values: impl Iterator<Item = String>, len: usize, name: &str) -> String {
    const PREVIEW: usize = 8;
    let body: Vec<String> = values.take(PREVIEW).collect();
    let ellipsis = if len > PREVIEW { ", ..." } else { "" };
    format!("{name}([{}{}], len={len})", body.join(", "), ellipsis)
}

/// Step-`1` slices with `start <= stop` and both bounds in-range are
/// contiguous zero-copy windows.
const fn contiguous_positive_window(
    start: isize,
    stop: isize,
    step: isize,
) -> Option<Range<usize>> {
    if step != 1 || start < 0 || stop < start {
        return None;
    }
    Some(start as usize..stop as usize)
}

fn validate_csr_offsets(offsets: &[i64], values_len: usize) -> PyResult<()> {
    // Parameter/content validation — GeometryError, never BufferError (D32:
    // empty offsets used to underflow then surface as BufferError/panic).
    if offsets.first() != Some(&0) || offsets.last() != Some(&(values_len as i64)) {
        return Err(GeometryError::new_err("invalid CSR offsets"));
    }
    if !offsets.is_sorted() {
        return Err(GeometryError::new_err("invalid CSR offsets"));
    }
    Ok(())
}

fn geometry_arrays_equal(left: &PyGeometryArray, right: &PyGeometryArray) -> bool {
    left.frame == right.frame
        && left.missing() == right.missing()
        && left.storage().len() == right.storage().len()
        && (Arc::ptr_eq(left.storage_arc(), right.storage_arc())
            || left
                .storage()
                .iter_shapes()
                .zip(right.storage().iter_shapes())
                .enumerate()
                .all(|(row, (left_shape, right_shape))| {
                    left.is_row_missing(row) || left_shape.as_ref() == right_shape.as_ref()
                }))
}

fn slice_rows(
    start: isize,
    step: isize,
    count: usize,
) -> impl ExactSizeIterator<Item = usize> + Clone {
    (0..count).map(move |offset| {
        let offset = isize::try_from(offset).expect("slice length fits in isize");
        usize::try_from(start + step * offset).expect("normalized slice rows are non-negative")
    })
}

/// Flat backing storage for a [`Groups`] container.
#[derive(Clone)]
enum GroupsValues {
    Int64(Arc<[i64]>),
    Geometry(Arc<PyGeometryArray>),
    Cells(Arc<PyCellArray>),
}

/// Shared CSR ragged container: one flat `values` payload plus row `offsets`.
/// ``groups[i]`` is a zero-copy row view; ``groups[s]`` shares the backing
/// with a sub-offset window. ``.values``/``.offsets``/``.counts`` expose the
/// Arrow ListArray columns for vectorized work without copying.
#[pyclass(
    name = "Groups",
    module = "gometry",
    frozen,
    sequence,
    generic,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct Groups {
    values: GroupsValues,
    /// `rows.len() + 1` absolute offsets into the FULL `values` buffer; row
    /// slicing narrows `rows` and never rebases int64 views (they stay
    /// zero-copy).
    offsets: Arc<[i64]>,
    rows: Range<usize>,
}

#[path = "vectors_ctor.rs"]
mod vectors_ctor;

impl Groups {
    /// Row `i`'s window into the full int64 values buffer.
    fn int64_window(&self, row: usize) -> Range<usize> {
        let start = self.offsets[self.rows.start + row] as usize;
        let end = self.offsets[self.rows.start + row + 1] as usize;
        start..end
    }

    fn int64_row_slice(&self, row: usize) -> &[i64] {
        let GroupsValues::Int64(values) = &self.values else {
            unreachable!("int64_row_slice requires Int64 backing");
        };
        &values[self.int64_window(row)]
    }

    fn geometry_indices_window(&self, row: usize) -> Range<usize> {
        self.int64_window(row)
    }

    fn geometry_row_view(&self, row: usize) -> PyGeometryArray {
        let GroupsValues::Geometry(values) = &self.values else {
            unreachable!("geometry_row_view requires Geometry backing");
        };
        values.gather_logical_row_range(self.geometry_indices_window(row))
    }

    fn cell_row_view(&self, row: usize) -> PyCellArray {
        let GroupsValues::Cells(values) = &self.values else {
            unreachable!("cell_row_view requires Cells backing");
        };
        values.logical_row_range(self.int64_window(row))
    }

    fn slice_rows(&self, rows: Range<usize>) -> Self {
        Self {
            values: self.values.clone(),
            offsets: Arc::clone(&self.offsets),
            rows,
        }
    }

    fn values_window(&self) -> Range<usize> {
        self.offsets[self.rows.start] as usize..self.offsets[self.rows.end] as usize
    }

    fn logical_offsets_bytes(&self) -> usize {
        (self.rows.len() + 1) * std::mem::size_of::<i64>()
    }

    fn geometry_values_view(&self) -> PyGeometryArray {
        let GroupsValues::Geometry(values) = &self.values else {
            unreachable!("geometry_values_view requires Geometry backing");
        };
        values.gather_logical_row_range(self.values_window())
    }

    fn logical_payload_bytes(&self) -> usize {
        let values_bytes = match &self.values {
            GroupsValues::Int64(_) => self.values_window().len() * std::mem::size_of::<i64>(),
            GroupsValues::Geometry(values) => {
                values.logical_coordinate_bytes_range(self.values_window())
            },
            GroupsValues::Cells(_) => self.values_window().len() * std::mem::size_of::<u64>(),
        };
        values_bytes + self.logical_offsets_bytes()
    }

    fn logical_heap_bytes(&self) -> usize {
        let values_bytes = match &self.values {
            GroupsValues::Int64(_) => self.values_window().len() * std::mem::size_of::<i64>(),
            GroupsValues::Geometry(values) => values.logical_heap_bytes_range(self.values_window()),
            GroupsValues::Cells(_) => self.values_window().len() * std::mem::size_of::<u64>(),
        };
        values_bytes + self.logical_offsets_bytes()
    }

    fn row_pyobject(
        &self,
        py: Python<'_>,
        slf: Bound<'_, Self>,
        row: usize,
    ) -> PyResult<Py<PyAny>> {
        match &self.values {
            GroupsValues::Int64(_) => frozen_i64_view(slf, |this| this.int64_row_slice(row)),
            GroupsValues::Geometry(_) => self.geometry_row_view(row).into_py_any(py),
            GroupsValues::Cells(_) => self.cell_row_view(row).into_py_any(py),
        }
    }

    fn groups_equal(&self, other: &Self) -> bool {
        if self.rows.len() != other.rows.len()
            || (0..self.rows.len())
                .any(|row| self.int64_window(row).len() != other.int64_window(row).len())
        {
            return false;
        }
        match (&self.values, &other.values) {
            (GroupsValues::Int64(left), GroupsValues::Int64(right)) => {
                left[self.values_window()] == right[other.values_window()]
            },
            (GroupsValues::Geometry(_), GroupsValues::Geometry(_)) => {
                geometry_arrays_equal(&self.geometry_values_view(), &other.geometry_values_view())
            },
            (GroupsValues::Cells(left), GroupsValues::Cells(right)) => left
                .logical_row_range(self.values_window())
                .logical_eq(&right.logical_row_range(other.values_window())),
            _ => false,
        }
    }

    /// Whether ragged row `row` equals the Python `item` (a per-row payload:
    /// int sequence / `GeometryArray` / `CellArray`, by backing type). The
    /// shared row comparator for membership (`in`, any row) and positional
    /// equality (`==`, row-aligned) — so the two can never disagree. Internal
    /// (not a `#[pymethods]` surface).
    fn row_matches(&self, row: usize, item: &Bound<'_, PyAny>) -> bool {
        match &self.values {
            GroupsValues::Int64(_) => {
                // Generic per-element iteration: rows come back to Python as
                // ndarrays (which fail a `PySequence` cast), and any iterable
                // of ints should compare equal to a row.
                let Ok(iter) = item.try_iter() else {
                    return false;
                };
                let values = self.int64_row_slice(row);
                let mut expected = values.iter();
                for candidate in iter {
                    let Ok(candidate) = candidate.and_then(|value| value.extract::<i64>()) else {
                        return false;
                    };
                    if expected.next() != Some(&candidate) {
                        return false;
                    }
                }
                expected.next().is_none()
            },
            GroupsValues::Geometry(_) => item.cast::<PyGeometryArray>().is_ok_and(|other| {
                geometry_arrays_equal(&self.geometry_row_view(row), other.get())
            }),
            GroupsValues::Cells(_) => item
                .cast::<PyCellArray>()
                .is_ok_and(|other| self.cell_row_view(row).logical_eq(other.get())),
        }
    }
}

impl HeapSize for Groups {
    fn heap_bytes(&self) -> usize {
        self.logical_heap_bytes()
    }
}

impl RowContainer for Groups {
    const LABEL: &'static str = "Groups";

    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
        let bound = Bound::new(py, self.clone())?;
        self.row_pyobject(py, bound, row)
    }
}

#[path = "vectors_iter.rs"]
mod vectors_iter;
pub(crate) use vectors_iter::GroupsIter;

#[pymethods]
impl Groups {
    /// Not constructed directly — the error points at the producers.
    #[new]
    fn no_direct_ctor() -> PyResult<Self> {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "Groups is returned by grouping operations (e.g. index queries, \
             self_intersections, line_interpolate_points), not constructed \
             directly",
        ))
    }

    // Sequences compare by value; like lists, they do not hash.
    #[expect(non_upper_case_globals, reason = "Python dunder name")]
    const __hash__: Option<Py<PyAny>> = None;
    #[classattr]
    #[expect(non_upper_case_globals, reason = "Python dunder name")]
    const __array_ufunc__: Option<Py<PyAny>> = None;

    /// Logical CSR payload in bytes: the selected flat values payload plus the
    /// ``len(self) + 1`` int64 offsets column. For geometry-valued groups this
    /// uses the values ``GeometryArray.nbytes`` and excludes geometry
    /// structural offsets, matching NumPy's payload-only convention.
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    fn nbytes(&self) -> usize {
        self.logical_payload_bytes()
    }

    /// ``sys.getsizeof`` support: the wrapper plus this group's logical CSR
    /// payload. Sliced groups report the visible values window and rebased
    /// logical offsets, not the whole shared backing allocation.
    fn __sizeof__(&self) -> usize {
        std::mem::size_of::<Self>() + self.logical_heap_bytes()
    }

    /// The flat backing column (int64 ndarray or `GeometryArray`).
    #[getter]
    fn values(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        match &slf.borrow().values {
            GroupsValues::Int64(_) => frozen_i64_view(slf, |this| {
                let GroupsValues::Int64(values) = &this.values else {
                    unreachable!("values getter int64 branch");
                };
                let start = this.offsets[this.rows.start] as usize;
                let end = this.offsets[this.rows.end] as usize;
                &values[start..end]
            }),
            // Mirror the int64 branch: return the LOGICAL window (matching the
            // rebased `offsets`), not the full backing — else a sliced
            // Geometry-valued groups break the CSR `values[offsets[i]:offsets[i+1]]`
            // invariant.
            GroupsValues::Geometry(values) => {
                let this = slf.borrow();
                let start = this.offsets[this.rows.start] as usize;
                let end = this.offsets[this.rows.end] as usize;
                let py = slf.py();
                if start == 0 && end == values.storage().len() {
                    values.as_ref().clone().into_py_any(py)
                } else {
                    values.gather_logical_row_range(start..end).into_py_any(py)
                }
            },
            GroupsValues::Cells(values) => {
                let this = slf.borrow();
                let start = this.offsets[this.rows.start] as usize;
                let end = this.offsets[this.rows.end] as usize;
                let py = slf.py();
                if start == 0 && end == values.len() {
                    values.as_ref().clone().into_py_any(py)
                } else {
                    values.logical_row_range(start..end).into_py_any(py)
                }
            },
        }
    }

    /// The `len(self) + 1` row boundaries into ``values`` (CSR offsets
    /// column): row ``i`` is ``values[offsets[i]:offsets[i + 1]]``.
    #[getter]
    fn offsets(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        if slf.borrow().rows.start == 0 {
            return frozen_i64_view(slf, |this| &this.offsets[0..=this.rows.end]);
        }
        let this = slf.borrow();
        let base = this.offsets[this.rows.start];
        int64_array(
            slf.py(),
            this.offsets[this.rows.start..=this.rows.end]
                .iter()
                .map(|&offset| offset - base)
                .collect(),
        )
    }

    /// Per-group element counts (`offsets[i + 1] - offsets[i]`).
    #[getter]
    fn counts(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let this = slf.borrow();
        int64_array(
            slf.py(),
            (this.rows.start..this.rows.end)
                .map(|row| this.offsets[row + 1] - this.offsets[row])
                .collect(),
        )
    }

    /// Expand integer CSR rows into parallel ``(row_ids, values)`` columns.
    ///
    /// The right column is a zero-copy read-only view of the flat CSR values;
    /// only the repeated row-id column is materialized. Row ids are positions
    /// in this logical ``Groups`` object, so sliced groups start again at zero.
    ///
    /// Returns
    /// -------
    /// tuple of numpy.ndarray
    ///     Parallel read-only int64 ``(row_ids, values)`` columns.
    ///
    /// Raises
    /// ------
    /// TypeError
    ///     If the groups contain geometry or cell rows rather than integers.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> idx = gm.SpatialIndex([
    /// ...     gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3), gm.box(10, 10, 11, 11)])
    /// >>> row_ids, values = idx.query(
    /// ...     gm.GeometryArray([gm.Point(1.5, 1.5), gm.Point(10.5, 10.5)])
    /// ... ).to_pairs()
    /// >>> (row_ids.tolist(), values.tolist())
    /// ([0, 0, 1], [0, 1, 2])
    fn to_pairs(slf: Bound<'_, Self>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        if !matches!(slf.borrow().values, GroupsValues::Int64(_)) {
            return Err(PyTypeError::new_err(
                "to_pairs is only available for integer Groups",
            ));
        }
        let py = slf.py();
        let row_ids = {
            let this = slf.borrow();
            let mut row_ids = Vec::with_capacity(this.values_window().len());
            for (logical_row, row) in (this.rows.start..this.rows.end).enumerate() {
                let logical_row = i64::try_from(logical_row)
                    .map_err(|_| PyOverflowError::new_err("row index does not fit in int64"))?;
                let count = (this.offsets[row + 1] - this.offsets[row]) as usize;
                row_ids.extend(std::iter::repeat_n(logical_row, count));
            }
            row_ids
        };
        let row_ids = int64_array(py, row_ids)?;
        let values = frozen_i64_view(slf, |this| {
            let GroupsValues::Int64(values) = &this.values else {
                unreachable!("to_pairs checked integer backing");
            };
            &values[this.values_window()]
        })?;
        Ok((row_ids, values))
    }

    /// Number of row groups.
    ///
    /// Returns
    /// -------
    /// int
    fn __len__(&self) -> usize {
        self.rows.len()
    }

    /// ``False`` only when there are zero groups.
    ///
    /// Returns
    /// -------
    /// bool
    fn __bool__(&self) -> bool {
        !self.rows.is_empty()
    }

    /// Iterate one row group at a time.
    ///
    /// Returns
    /// -------
    /// iterator
    fn __iter__(slf: PyRef<'_, Self>) -> GroupsIter {
        GroupsIter::new((*slf).clone(), false)
    }

    /// Iterate row groups in reverse order.
    ///
    /// Returns
    /// -------
    /// iterator
    fn __reversed__(slf: PyRef<'_, Self>) -> GroupsIter {
        GroupsIter::new((*slf).clone(), true)
    }

    /// Select groups by integer or slice.
    ///
    /// An ``int`` returns one group's values (for example an ``int64``
    /// ndarray of matched ids). A ``slice`` returns a rebased ``Groups``.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray or Groups
    fn __getitem__(slf: Bound<'_, Self>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let parsed = {
            let this = slf.borrow();
            parse_row_index_or_slice(index, this.rows.len(), Self::LABEL)?
        };
        match parsed {
            RowIndexOrSlice::Index(row) => slf.borrow().row_pyobject(slf.py(), slf, row),
            RowIndexOrSlice::Slice {
                start,
                stop,
                step,
                count,
            } => {
                let this = slf.borrow();
                let groups = if let Some(window) = contiguous_positive_window(start, stop, step) {
                    this.slice_rows(this.rows.start + window.start..this.rows.start + window.end)
                } else {
                    let rows = slice_rows(start, step, count);
                    match &this.values {
                        GroupsValues::Int64(_) => {
                            let value_count =
                                rows.clone().map(|row| this.int64_window(row).len()).sum();
                            let mut ids = Vec::with_capacity(value_count);
                            let mut offsets = Vec::with_capacity(count + 1);
                            offsets.push(0_i64);
                            for row in rows {
                                ids.extend_from_slice(this.int64_row_slice(row));
                                offsets.push(i64::try_from(ids.len()).map_err(|_| {
                                    PyOverflowError::new_err("offset does not fit in int64")
                                })?);
                            }
                            let rows = 0..offsets.len() - 1;
                            Self {
                                values: GroupsValues::Int64(ids.into()),
                                offsets: offsets.into(),
                                rows,
                            }
                        },
                        GroupsValues::Geometry(values) => {
                            debug_assert!(
                                !values.has_missing(),
                                "Groups geometry values are compact present rows, never masked"
                            );
                            let value_count = rows
                                .clone()
                                .map(|row| this.geometry_indices_window(row).len())
                                .sum();
                            let mut offsets = Vec::with_capacity(count + 1);
                            offsets.push(0_i64);
                            let mut shapes = Vec::with_capacity(value_count);
                            for row in rows {
                                let row_values = values
                                    .gather_logical_row_range(this.geometry_indices_window(row));
                                shapes.extend(
                                    row_values.storage().iter_shapes().map(Cow::into_owned),
                                );
                                offsets.push(i64::try_from(shapes.len()).map_err(|_| {
                                    PyOverflowError::new_err("offset does not fit in int64")
                                })?);
                            }
                            let flat = Arc::new(PyGeometryArray::from_shapes(
                                shapes,
                                values.frame.clone(),
                            ));
                            let rows = offsets.len() - 1;
                            Self::from_geometry_csr(flat, offsets.into(), 0..rows)?
                        },
                        GroupsValues::Cells(values) => {
                            let kind = values.kind();
                            Self::from_cell_rows(
                                kind,
                                rows.map(|row| this.cell_row_view(row).logical_ids()),
                            )?
                        },
                    }
                };
                groups.into_py_any(slf.py())
            },
        }
    }

    /// Whether any group equals ``item`` (whole-row value equality).
    ///
    /// Returns
    /// -------
    /// bool
    fn __contains__(&self, item: &Bound<'_, PyAny>) -> bool {
        (0..self.rows.len()).any(|row| self.row_matches(row, item))
    }

    /// First index of an equal row in ``[start, stop)``.
    ///
    /// Parameters
    /// ----------
    /// value : object
    ///     The row value to locate.
    /// start : int, default 0
    ///     First position searched.
    /// stop : int, optional
    ///     One past the last position searched.
    ///
    /// Returns
    /// -------
    /// int
    ///     The first matching position.
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If no row in the window equals ``value``.
    #[pyo3(signature = (value, start = 0, stop = None), text_signature = "($self, value, start=0, stop=None)")]
    fn index(&self, value: &Bound<'_, PyAny>, start: i64, stop: Option<i64>) -> PyResult<usize> {
        let len = self.rows.len();
        let clamp = |bound: i64| -> usize {
            let resolved = if bound < 0 {
                bound + i64::try_from(len).unwrap_or(i64::MAX)
            } else {
                bound
            };
            usize::try_from(resolved.max(0)).unwrap_or(0).min(len)
        };
        let start = clamp(start);
        let stop = stop.map_or(len, clamp);
        if start < stop
            && let Some(row) = (start..stop).find(|&row| self.row_matches(row, value))
        {
            return Ok(row);
        }
        let value = value
            .repr()
            .and_then(|repr| repr.extract::<String>())
            .unwrap_or_else(|_| "value".to_owned());
        Err(PyValueError::new_err(format!("{value} is not in Groups")))
    }

    /// Number of rows equal to ``value``.
    ///
    /// Parameters
    /// ----------
    /// value : object
    ///     The row value to count.
    ///
    /// Returns
    /// -------
    /// int
    fn count(&self, value: &Bound<'_, PyAny>) -> usize {
        (0..self.rows.len())
            .filter(|&row| self.row_matches(row, value))
            .count()
    }

    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if let Ok(groups) = other.cast::<Self>() {
            let groups = groups.get();
            return self.groups_equal(groups).into_py_any(py);
        }
        let Ok(sequence) = other.cast::<PySequence>() else {
            return Ok(py.NotImplemented());
        };
        if sequence.len()? != self.rows.len() {
            return false.into_py_any(py);
        }
        // Positional, row-aligned: row `i` must equal `sequence[i]` (NOT mere
        // membership — order and multiplicity are significant, as for any
        // Python sequence equality).
        for row in 0..self.rows.len() {
            let item = sequence.get_item(row)?;
            if !self.row_matches(row, &item) {
                return false.into_py_any(py);
            }
        }
        true.into_py_any(py)
    }

    fn __repr__(&self) -> String {
        match &self.values {
            GroupsValues::Int64(_) => preview(
                (0..self.rows.len()).map(|row| format!("{:?}", self.int64_row_slice(row))),
                self.rows.len(),
                "Groups",
            ),
            GroupsValues::Geometry(_) => preview(
                (0..self.rows.len()).map(|row| self.geometry_row_view(row).__repr__()),
                self.rows.len(),
                "Groups",
            ),
            GroupsValues::Cells(_) => preview(
                (0..self.rows.len()).map(|row| self.cell_row_view(row).__repr__()),
                self.rows.len(),
                "Groups",
            ),
        }
    }

    /// Copy into a plain nested Python list.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> groups = gm.GeometryArray([gm.box(0, 0, 2, 2)]).triangulate(method='earcut')
    /// >>> [g.to_wkt() for g in groups.to_list()[0]]
    /// ['POLYGON ((0 2, 0 0, 2 0, 0 2))', 'POLYGON ((2 0, 2 2, 0 2, 2 0))']
    fn to_list(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let rows: Vec<Py<PyAny>> = (0..self.rows.len())
            .map(|row| match &self.values {
                GroupsValues::Int64(_) => self.int64_row_slice(row).to_vec().into_py_any(py),
                GroupsValues::Geometry(_) => {
                    let items = self.geometry_row_view(row).materialized_objects(py)?;
                    PyList::new(py, items)?.into_py_any(py)
                },
                GroupsValues::Cells(_) => {
                    let items = self.cell_row_view(row).to_cell_list(py)?;
                    PyList::new(py, items)?.into_py_any(py)
                },
            })
            .collect::<PyResult<_>>()?;
        PyList::new(py, rows)?.into_py_any(py)
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let module = crate::gometry_lib_module(py)?;
        // Pickle the LOGICAL groups: rebase offsets to start at 0 and slice the
        // backing to the window, so a sliced Groups round-trips (the
        // `_unpickle_*` validators require offsets[0]==0 and last==values_len).
        let base = self.offsets[self.rows.start];
        let start = base as usize;
        let end = self.offsets[self.rows.end] as usize;
        let rebased: Vec<i64> = self.offsets[self.rows.start..=self.rows.end]
            .iter()
            .map(|&offset| offset - base)
            .collect();
        match &self.values {
            GroupsValues::Int64(values) => {
                let args = (values[start..end].to_vec(), rebased).into_py_any(py)?;
                Ok((
                    module
                        .getattr(pyo3::intern!(py, "_unpickle_int64_groups"))?
                        .unbind(),
                    args,
                ))
            },
            GroupsValues::Geometry(values) => {
                let windowed = if start == 0 && end == values.storage().len() {
                    values.as_ref().clone()
                } else {
                    values.gather_logical_row_range(start..end)
                };
                let args = (Py::new(py, windowed)?, rebased).into_py_any(py)?;
                Ok((
                    module
                        .getattr(pyo3::intern!(py, "_unpickle_geometry_groups"))?
                        .unbind(),
                    args,
                ))
            },
            GroupsValues::Cells(values) => {
                let windowed = if start == 0 && end == values.len() {
                    values.as_ref().clone()
                } else {
                    values.logical_row_range(start..end)
                };
                let args = (Py::new(py, windowed)?, rebased).into_py_any(py)?;
                Ok((
                    module
                        .getattr(pyo3::intern!(py, "_unpickle_cell_groups"))?
                        .unbind(),
                    args,
                ))
            },
        }
    }
}

#[pyfunction]
pub(crate) fn _unpickle_int64_groups(
    ids: &Bound<'_, PyAny>,
    offsets: &Bound<'_, PyAny>,
) -> PyResult<Groups> {
    let ids = crate::collect_i64_sequence(ids, "Groups pickle ids")?;
    let offsets = crate::collect_i64_sequence(offsets, "Groups pickle offsets")?;
    validate_csr_offsets(&offsets, ids.len())?;
    let rows = 0..offsets.len() - 1;
    Ok(Groups {
        values: GroupsValues::Int64(ids.into()),
        offsets: offsets.into(),
        rows,
    })
}

#[pyfunction]
pub(crate) fn _unpickle_geometry_groups(
    values: Py<PyGeometryArray>,
    offsets: &Bound<'_, PyAny>,
) -> PyResult<Groups> {
    let offsets = crate::collect_i64_sequence(offsets, "geometry Groups pickle offsets")?;
    Python::attach(|py| {
        let array = values.bind(py).borrow().clone();
        // Validate CSR before computing the row range — empty offsets must not
        // panic on `len() - 1` (debug) before the BufferError path runs.
        validate_csr_offsets(&offsets, array.storage().len())?;
        let offsets: Arc<[i64]> = offsets.into();
        let rows = 0..offsets.len() - 1;
        Groups::from_geometry_csr(Arc::new(array), offsets, rows)
    })
}

#[pyfunction]
pub(crate) fn _unpickle_cell_groups(
    values: Py<PyCellArray>,
    offsets: &Bound<'_, PyAny>,
) -> PyResult<Groups> {
    let offsets = crate::collect_i64_sequence(offsets, "cell Groups pickle offsets")?;
    Python::attach(|py| {
        let array = values.bind(py).borrow().clone();
        validate_csr_offsets(&offsets, array.len())?;
        let offsets: Arc<[i64]> = offsets.into();
        let rows = 0..offsets.len() - 1;
        Groups::from_cell_csr(Arc::new(array), offsets, rows)
    })
}

/// Internal zero-copy buffer holder for `f64` coordinate columns (Arrow
/// export). Not part of the public surface — constructed from Rust only
/// and consumed through PEP-3118 (`pa.py_buffer`).
#[pyclass(name = "_Float64Buffer", module = "gometry", frozen)]
pub(crate) struct Float64Buffer {
    storage: Arc<[f64]>,
    range: Range<usize>,
}

impl Float64Buffer {
    /// Zero-copy window over shared storage.
    pub(crate) fn view(storage: Arc<[f64]>, range: Range<usize>) -> PyResult<Self> {
        if range.start > range.end || range.end > storage.len() {
            return Err(PyBufferError::new_err("buffer window out of range"));
        }
        Ok(Self { storage, range })
    }

    fn as_slice(&self) -> &[f64] {
        &self.storage[self.range.clone()]
    }
}

#[pymethods]
impl Float64Buffer {
    fn __len__(&self) -> usize {
        self.range.len()
    }

    /// # Safety
    /// Buffer-protocol slot; `CPython` guarantees the view pointer.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let this = slf.get();
        let slice = this.as_slice();
        // SAFETY: the slice is the frozen pyclass's own storage; the owner
        // handle passed in keeps it alive for the view.
        unsafe {
            fill_typed_view(
                view,
                flags,
                slice.as_ptr().cast(),
                slice.len(),
                std::mem::size_of::<f64>(),
                c"d",
                slf.into_any(),
                "gometry vectors are read-only",
            )
        }
    }

    /// # Safety
    /// Buffer-protocol slot paired with `__getbuffer__`.
    unsafe fn __releasebuffer__(&self, view: *mut ffi::Py_buffer) {
        // SAFETY: paired slot — the view came from `__getbuffer__`.
        unsafe { release_typed_view(view) }
    }
}

/// Internal zero-copy buffer holder for `i32` columns (Arrow list
/// offsets). Not part of the public surface — constructed from Rust only
/// and consumed through PEP-3118 (`pa.py_buffer`).
#[pyclass(name = "_Int32Buffer", module = "gometry", frozen)]
pub(crate) struct Int32Buffer {
    storage: Arc<[i32]>,
}

impl Int32Buffer {
    pub(crate) const fn new(storage: Arc<[i32]>) -> Self {
        Self { storage }
    }

    fn as_slice(&self) -> &[i32] {
        &self.storage
    }
}

#[pymethods]
impl Int32Buffer {
    /// # Safety
    /// Buffer-protocol slot; `CPython` guarantees the view pointer.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let this = slf.get();
        let slice = this.as_slice();
        // SAFETY: the slice is the frozen pyclass's own storage; the owner
        // handle passed in keeps it alive for the view.
        unsafe {
            fill_typed_view(
                view,
                flags,
                slice.as_ptr().cast(),
                slice.len(),
                std::mem::size_of::<i32>(),
                c"i",
                slf.into_any(),
                "gometry vectors are read-only",
            )
        }
    }

    /// # Safety
    /// Buffer-protocol slot paired with `__getbuffer__`.
    unsafe fn __releasebuffer__(&self, view: *mut ffi::Py_buffer) {
        // SAFETY: paired slot — the view came from `__getbuffer__`.
        unsafe { release_typed_view(view) }
    }
}

/// Register the public result containers on the module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Groups>()?;
    module.add_class::<GroupsIter>()?;
    module.add_function(wrap_pyfunction!(_unpickle_int64_groups, module)?)?;
    module.add_function(wrap_pyfunction!(_unpickle_geometry_groups, module)?)?;
    module.add_function(wrap_pyfunction!(_unpickle_cell_groups, module)?)?;
    Ok(())
}
