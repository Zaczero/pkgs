#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
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

use std::ffi::c_int;
use std::ops::Range;
use std::sync::Arc;

use pyo3::exceptions::{PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PySequence};
use pyo3::{IntoPyObjectExt as _, ffi};

use crate::array::PyGeometryArray;
use crate::boundary::coordinates::measure_storage_payload_bytes;
use crate::geometry::Shape;
use crate::py::buffer::{fill_typed_view, release_typed_view};
use crate::py::cells::{GridKind, PyCellArray};
use crate::py::errors::GeometryError;
use crate::py::numpy::{ImmutableI64Owner, frozen_i64_owner, frozen_i64_view, int64_array};
use crate::py::row::{
    RowContainer, RowIndexOrSlice, RowIterState, parse_row_index_or_slice, slice_row_indices,
};
use crate::py::sizeof::python_sizeof;
use crate::{CsrBuilder, HeapSize};

/// Format a short preview list of already-rendered row strings.
///
/// Callers own the per-row rendering so geometry/cell groups use the public
/// Python `__repr__` path rather than Rust `Debug`.
fn preview(
    values: impl Iterator<Item = PyResult<String>>,
    len: usize,
    name: &str,
) -> PyResult<String> {
    const PREVIEW: usize = 8;
    let body = values.take(PREVIEW).collect::<pyo3::PyResult<Vec<_>>>()?;
    let ellipsis = if len > PREVIEW { ", ..." } else { "" };
    Ok(format!(
        "{name}([{}{}], len={len})",
        body.join(", "),
        ellipsis
    ))
}

#[cfg(test)]
mod preview_tests {
    use super::preview;

    #[test]
    fn preview_only_renders_visible_rows_and_propagates_their_errors() {
        let rows = (0..100).map(|row| {
            assert!(row < 8, "hidden row was rendered");
            Ok(row.to_string())
        });
        assert_eq!(
            preview(rows, 100, "Groups").unwrap(),
            "Groups([0, 1, 2, 3, 4, 5, 6, 7, ...], len=100)"
        );
        let rows = std::iter::once(Err(pyo3::exceptions::PyValueError::new_err("row failed")));
        preview(rows, 1, "Groups").unwrap_err();
    }
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

fn validate_csr_offsets(offsets: &[i64], values_len: usize) -> PyResult<usize> {
    // Parameter/content validation — GeometryError, never BufferError (D32:
    // empty offsets used to underflow then surface as BufferError/panic).
    let values_len = i64::try_from(values_len)
        .map_err(|_| GeometryError::new_err("CSR values length does not fit in int64"))?;
    if offsets.first() != Some(&0) || offsets.last() != Some(&values_len) {
        return Err(GeometryError::new_err("invalid CSR offsets"));
    }
    if !offsets.is_sorted() {
        return Err(GeometryError::new_err("invalid CSR offsets"));
    }
    Ok(offsets.len().saturating_sub(1))
}

fn geometry_arrays_equal(left: &PyGeometryArray, right: &PyGeometryArray) -> PyResult<bool> {
    if left.frame != right.frame || left.missing() != right.missing() || left.len() != right.len() {
        return Ok(false);
    }
    if Arc::ptr_eq(left.storage_arc(), right.storage_arc()) {
        return Ok(true);
    }
    for (left, right) in left
        .row_operation()
        .rows_with_missing(|| ())
        .zip(right.row_operation().rows_with_missing(|| ()))
    {
        let equal = match (left, right) {
            (crate::array::ArrayRow::Missing { .. }, crate::array::ArrayRow::Missing { .. }) => {
                true
            },
            (crate::array::ArrayRow::Present(left), crate::array::ArrayRow::Present(right)) => left
                .row()
                .try_with_shape(|left| right.row().with_shape(|right| left == right))?,
            _ => false,
        };
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Flat backing storage for a [`Groups`] container.
#[derive(Clone)]
enum GroupsValues {
    Int64(Arc<[i64]>),
    Geometry(Arc<PyGeometryArray>),
    Cells(Arc<PyCellArray>),
}

/// Shared CSR ragged container: one flat `values` payload plus row `offsets`.
/// The values payload keeps its actual type (int64, geometry array, or cell
/// array). A contiguous values window may share backing; offsets for a window
/// starting after row zero are rebased, and counts are materialized.
#[pyclass(
    name = "Groups",
    module = "gometry",
    frozen,
    immutable_type,
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

enum GroupsContainsNeedle {
    NoMatch,
    Int64(Vec<i64>),
    Geometry(PyGeometryArray),
    Cells(PyCellArray),
}

// Frozen + immutable Arc<[i64]> backing: the only owner admitted to
// `frozen_i64_view` (Item C). A mutable pyclass cannot implement Sealed.
impl frozen_i64_owner::Sealed for Groups {}
impl ImmutableI64Owner for Groups {}

mod ctor;

impl Groups {
    fn visible_offsets(&self) -> &[i64] {
        self.offsets
            .get(self.rows.start..=self.rows.end)
            .expect("Groups rows are bounded by their admitted offsets")
    }

    /// Read one admitted adjacent offset pair.
    fn row_values_window_at(offsets: &[i64], row_offset: usize) -> Range<usize> {
        let &[start, end] = offsets
            .get(row_offset..)
            .and_then(|offsets| offsets.first_chunk::<2>())
            .expect("Groups row offsets are bounded by their admitted CSR range");
        start as usize..end as usize
    }

    /// Row `i`'s logical value window in the backing payload.
    fn row_values_window(&self, row: usize) -> Range<usize> {
        // Callers admit `row` against this visible row range before rebasing it.
        let offset = self.rows.start.wrapping_add(row);
        Self::row_values_window_at(&self.offsets, offset)
    }

    fn int64_row_slice(&self, row: usize) -> &[i64] {
        let GroupsValues::Int64(values) = &self.values else {
            unreachable!("int64_row_slice requires Int64 backing");
        };
        values
            .get(self.row_values_window(row))
            .expect("Groups row values are bounded by admitted offsets")
    }

    fn geometry_row_view(&self, row: usize) -> PyResult<PyGeometryArray> {
        let GroupsValues::Geometry(values) = &self.values else {
            unreachable!("geometry_row_view requires Geometry backing");
        };
        values
            .gather_logical_row_range(self.row_values_window(row))
            .map_err(PyErr::from)
    }

    fn cell_row_view(&self, row: usize) -> PyCellArray {
        let GroupsValues::Cells(values) = &self.values else {
            unreachable!("cell_row_view requires Cells backing");
        };
        values.logical_row_range(self.row_values_window(row))
    }

    fn slice_rows(&self, rows: Range<usize>) -> Self {
        Self {
            values: self.values.clone(),
            offsets: Arc::clone(&self.offsets),
            rows,
        }
    }

    fn values_window_in(offsets: &[i64]) -> Range<usize> {
        match offsets {
            [only] => {
                let only = *only as usize;
                only..only
            },
            [start, .., end] => *start as usize..*end as usize,
            [] => unreachable!("Groups rows include their starting offset"),
        }
    }

    fn values_window(&self) -> Range<usize> {
        Self::values_window_in(self.visible_offsets())
    }

    fn logical_offsets_bytes(&self) -> usize {
        self.rows
            .len()
            .wrapping_add(1)
            .wrapping_mul(std::mem::size_of::<i64>())
    }

    pub(crate) fn geometry_values_view(&self) -> PyResult<Option<PyGeometryArray>> {
        let GroupsValues::Geometry(values) = &self.values else {
            return Ok(None);
        };
        Ok(Some(
            values
                .gather_logical_row_range(self.values_window())
                .map_err(PyErr::from)?,
        ))
    }

    fn logical_payload_pyint<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyInt>> {
        let rows = self.values_window();
        let value_width = match &self.values {
            GroupsValues::Int64(_) => std::mem::size_of::<i64>(),
            GroupsValues::Cells(_) => std::mem::size_of::<u64>(),
            GroupsValues::Geometry(values) => {
                let bytes =
                    measure_storage_payload_bytes(py, values.storage(), values.missing(), rows)?;
                return Ok(bytes
                    .add_native_bytes(py, self.logical_offsets_bytes())?
                    .into_pyint(py));
            },
        };
        // These fixed-width ranges are backed by Vec-sized owners. Each byte
        // count is at most isize::MAX, so their sum fits usize.
        let value_bytes = rows.len().wrapping_mul(value_width);
        Ok(PyInt::new(
            py,
            value_bytes.wrapping_add(self.logical_offsets_bytes()),
        ))
    }

    fn logical_heap_bytes(&self) -> usize {
        let values_bytes = match &self.values {
            GroupsValues::Int64(_) => crate::heap_size::ARC_CONTROL_BYTES.saturating_add(
                self.values_window()
                    .len()
                    .saturating_mul(std::mem::size_of::<i64>()),
            ),
            GroupsValues::Geometry(values) => crate::heap_size::ARC_CONTROL_BYTES
                .saturating_add(std::mem::size_of::<PyGeometryArray>())
                .saturating_add(values.logical_heap_bytes_range(self.values_window())),
            GroupsValues::Cells(values) => crate::heap_size::ARC_CONTROL_BYTES
                .saturating_add(std::mem::size_of::<PyCellArray>())
                .saturating_add(values.logical_heap_bytes_range(self.values_window())),
        };
        values_bytes
            .saturating_add(crate::heap_size::ARC_CONTROL_BYTES)
            .saturating_add(self.logical_offsets_bytes())
    }

    fn row_pyobject(
        &self,
        py: Python<'_>,
        slf: Bound<'_, Self>,
        row: usize,
    ) -> PyResult<Py<PyAny>> {
        match &self.values {
            GroupsValues::Int64(_) => frozen_i64_view(slf, |this| this.int64_row_slice(row)),
            GroupsValues::Geometry(_) => self.geometry_row_view(row)?.into_py_any(py),
            GroupsValues::Cells(_) => self.cell_row_view(row).into_py_any(py),
        }
    }

    fn groups_equal(&self, other: &Self) -> PyResult<bool> {
        if self.rows.len() != other.rows.len()
            || (0..self.rows.len())
                .any(|row| self.row_values_window(row).len() != other.row_values_window(row).len())
        {
            return Ok(false);
        }
        match (&self.values, &other.values) {
            (GroupsValues::Int64(left), GroupsValues::Int64(right)) => {
                let left = left
                    .get(self.values_window())
                    .expect("Groups values are bounded by their admitted offsets");
                let right = right
                    .get(other.values_window())
                    .expect("Groups values are bounded by their admitted offsets");
                Ok(left == right)
            },
            (GroupsValues::Geometry(_), GroupsValues::Geometry(_)) => geometry_arrays_equal(
                self.geometry_values_view()?
                    .as_ref()
                    .expect("geometry backing is known from the match"),
                other
                    .geometry_values_view()?
                    .as_ref()
                    .expect("geometry backing is known from the match"),
            ),
            (GroupsValues::Cells(left), GroupsValues::Cells(right)) => Ok(left
                .logical_row_range(self.values_window())
                .logical_eq(&right.logical_row_range(other.values_window()))),
            _ => Ok(false),
        }
    }

    /// Whether ragged row `row` equals the Python `item` (a per-row payload:
    /// int sequence / `GeometryArray` / `CellArray`, by backing type). The
    /// shared row comparator for membership (`in`, any row) and positional
    /// equality (`==`, row-aligned) — so the two can never disagree. Internal
    /// (not a `#[pymethods]` surface).
    fn parse_int64_row_item(item: &Bound<'_, PyAny>) -> PyResult<Option<Vec<i64>>> {
        let iter = match item.try_iter() {
            Ok(iter) => iter,
            Err(error)
                if error.is_instance_of::<PyTypeError>(item.py())
                    && item
                        .getattr_opt(pyo3::intern!(item.py(), "__iter__"))?
                        .is_none()
                    && item
                        .getattr_opt(pyo3::intern!(item.py(), "__getitem__"))?
                        .is_none() =>
            {
                return Ok(None);
            },
            Err(error) => return Err(error),
        };
        let mut values = Vec::new();
        crate::reserve_from_py_len(item, &mut values)?;
        for (position, candidate) in iter.enumerate() {
            crate::check_python_signals(item.py(), position)?;
            let candidate = candidate?;
            if candidate.cast_exact::<pyo3::types::PyBool>().is_ok() {
                return Ok(None);
            }
            let index = match crate::py_index_object("row value", &candidate) {
                Ok(index) => index,
                Err(error)
                    if error.is_instance_of::<PyTypeError>(item.py())
                        && candidate
                            .getattr_opt(pyo3::intern!(item.py(), "__index__"))?
                            .is_none() =>
                {
                    return Ok(None);
                },
                Err(error) => return Err(error),
            };
            let Ok(candidate) = index.extract::<i64>() else {
                return Ok(None);
            };
            values.push(candidate);
        }
        Ok(Some(values))
    }

    fn int64_row_matches(&self, row: usize, expected: &[i64]) -> bool {
        let values = self.int64_row_slice(row);
        values == expected
    }

    fn row_matches(&self, row: usize, item: &Bound<'_, PyAny>) -> PyResult<bool> {
        match &self.values {
            GroupsValues::Int64(_) => Ok(Self::parse_int64_row_item(item)?
                .is_some_and(|expected| self.int64_row_matches(row, &expected))),
            GroupsValues::Geometry(_) => {
                let Ok(other) = item.cast::<PyGeometryArray>() else {
                    return Ok(false);
                };
                geometry_arrays_equal(&self.geometry_row_view(row)?, other.get())
            },
            GroupsValues::Cells(_) => {
                let Ok(other) = item.cast::<PyCellArray>() else {
                    return Ok(false);
                };
                Ok(self.cell_row_view(row).logical_eq(other.get()))
            },
        }
    }

    fn admit_contains_needle(&self, item: &Bound<'_, PyAny>) -> PyResult<GroupsContainsNeedle> {
        match &self.values {
            GroupsValues::Int64(_) => Ok(Self::parse_int64_row_item(item)?
                .map_or(GroupsContainsNeedle::NoMatch, GroupsContainsNeedle::Int64)),
            GroupsValues::Geometry(_) => Ok(item
                .cast::<PyGeometryArray>()
                .ok()
                .map_or(GroupsContainsNeedle::NoMatch, |other| {
                    GroupsContainsNeedle::Geometry(other.get().clone())
                })),
            GroupsValues::Cells(_) => Ok(item
                .cast::<PyCellArray>()
                .ok()
                .map_or(GroupsContainsNeedle::NoMatch, |other| {
                    GroupsContainsNeedle::Cells(other.get().clone())
                })),
        }
    }

    fn contains_needle(&self, needle: GroupsContainsNeedle) -> PyResult<bool> {
        match needle {
            GroupsContainsNeedle::NoMatch => Ok(false),
            GroupsContainsNeedle::Int64(expected) => {
                Ok((0..self.rows.len()).any(|row| self.int64_row_matches(row, &expected)))
            },
            GroupsContainsNeedle::Geometry(expected) => {
                for row in 0..self.rows.len() {
                    if geometry_arrays_equal(&self.geometry_row_view(row)?, &expected)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            },
            GroupsContainsNeedle::Cells(expected) => {
                for row in 0..self.rows.len() {
                    if self.cell_row_view(row).logical_eq(&expected) {
                        return Ok(true);
                    }
                }
                Ok(false)
            },
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

mod iter;
pub(crate) use iter::GroupsIter;

#[pymethods]
impl Groups {
    /// Not constructed directly — the error points at the producers.
    #[new]
    fn no_direct_ctor() -> PyResult<Self> {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "Groups is returned by grouping operations (e.g. index queries, \
             self_intersections, line_interpolate(..., count=...)), not constructed \
             directly",
        ))
    }

    // Sequences compare by value; like lists, they do not hash.
    #[expect(non_upper_case_globals, reason = "Python dunder name")]
    const __hash__: Option<Py<PyAny>> = None;

    fn __copy__<'py>(slf: &Bound<'py, Self>) -> Bound<'py, Self> {
        slf.clone()
    }

    #[pyo3(signature = (memo))]
    fn __deepcopy__<'py>(slf: &Bound<'py, Self>, memo: &Bound<'py, PyAny>) -> Bound<'py, Self> {
        let _ = memo;
        slf.clone()
    }

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
    fn nbytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyInt>> {
        self.logical_payload_pyint(py)
    }

    /// ``sys.getsizeof`` support: Python's allocation base plus this group's
    /// logical CSR payload. Sliced groups report the visible values window and
    /// rebased logical offsets, not the whole shared backing allocation.
    ///
    /// Raises
    /// ------
    /// OverflowError
    ///     If the reported size exceeds Python's ``Py_ssize_t`` range.
    fn __sizeof__(slf: &Bound<'_, Self>) -> PyResult<isize> {
        python_sizeof(slf.py(), slf.as_any(), slf.borrow().logical_heap_bytes())
    }

    /// The logical flat payload window (int64 ndarray, `GeometryArray`, or
    /// `CellArray`), with values in group order. A contiguous source window
    /// can share backing; gathered values may be materialized.
    ///
    /// Returns
    /// -------
    /// Group values
    ///     The selected flat payload window. Numeric values are read-only int64;
    ///     geometry and cell payloads retain their immutable array types.
    #[getter]
    fn values(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        match &slf.borrow().values {
            GroupsValues::Int64(_) => frozen_i64_view(slf, |this| {
                let GroupsValues::Int64(values) = &this.values else {
                    unreachable!("values getter int64 branch");
                };
                let offsets = this
                    .offsets
                    .get(this.rows.start..=this.rows.end)
                    .expect("Groups rows are bounded by their admitted offsets");
                let start = *offsets
                    .first()
                    .expect("Groups rows include their starting offset")
                    as usize;
                let end = *offsets
                    .last()
                    .expect("Groups rows include their terminal offset")
                    as usize;
                values
                    .get(start..end)
                    .expect("Groups offsets are bounded by their int64 values")
            }),
            // Mirror the int64 branch: return the LOGICAL window (matching the
            // rebased `offsets`), not the full backing — else a sliced
            // Geometry-valued groups break the CSR `values[offsets[i]:offsets[i+1]]`
            // invariant.
            GroupsValues::Geometry(values) => {
                let this = slf.borrow();
                let offsets = this
                    .offsets
                    .get(this.rows.start..=this.rows.end)
                    .expect("Groups rows are bounded by their admitted offsets");
                let start = *offsets
                    .first()
                    .expect("Groups rows include their starting offset")
                    as usize;
                let end = *offsets
                    .last()
                    .expect("Groups rows include their terminal offset")
                    as usize;
                let py = slf.py();
                if start == 0 && end == values.len() {
                    values.as_ref().clone().into_py_any(py)
                } else {
                    values.gather_logical_row_range(start..end)?.into_py_any(py)
                }
            },
            GroupsValues::Cells(values) => {
                let this = slf.borrow();
                let offsets = this
                    .offsets
                    .get(this.rows.start..=this.rows.end)
                    .expect("Groups rows are bounded by their admitted offsets");
                let start = *offsets
                    .first()
                    .expect("Groups rows include their starting offset")
                    as usize;
                let end = *offsets
                    .last()
                    .expect("Groups rows include their terminal offset")
                    as usize;
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
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     A read-only int64 array of row boundaries. Slices are rebased to
    ///     start at zero; a slice starting after row zero owns a rebased copy.
    #[getter]
    fn offsets(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        if slf.borrow().rows.start == 0 {
            return frozen_i64_view(slf, |this| {
                this.offsets
                    .get(0..=this.rows.end)
                    .expect("Groups rows are bounded by their admitted offsets")
            });
        }
        let this = slf.borrow();
        let source_offsets = this
            .offsets
            .get(this.rows.start..=this.rows.end)
            .expect("Groups rows are bounded by their admitted offsets");
        let base = *source_offsets
            .first()
            .expect("Groups rows include their starting offset");
        let mut offsets = Vec::with_capacity(this.rows.len().saturating_add(1));
        for &offset in source_offsets {
            offsets.push(offset.wrapping_sub(base));
        }
        int64_array(slf.py(), offsets)
    }

    /// Per-group element counts (`offsets[i + 1] - offsets[i]`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     A materialized, read-only int64 count for each group in row order.
    #[getter]
    fn counts(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let this = slf.borrow();
        let mut counts = Vec::with_capacity(this.rows.len());
        let offsets = this
            .offsets
            .get(this.rows.start..=this.rows.end)
            .expect("Groups rows are bounded by their admitted offsets");
        for pair in offsets.windows(2) {
            let [start, end] = pair else {
                unreachable!("two-offset windows contain two values");
            };
            counts.push(end.wrapping_sub(*start));
        }
        int64_array(slf.py(), counts)
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
            let offsets = this.visible_offsets();
            let values_window = Self::values_window_in(offsets);
            let mut row_ids = Vec::with_capacity(values_window.len());
            for (logical_row, pair) in offsets.windows(2).enumerate() {
                let [start, end] = pair else {
                    unreachable!("two-offset windows contain two values");
                };
                let count = end.wrapping_sub(*start) as usize;
                row_ids.extend(std::iter::repeat_n(logical_row as i64, count));
            }
            row_ids
        };
        let row_ids = int64_array(py, row_ids)?;
        let values = frozen_i64_view(slf, |this| {
            let GroupsValues::Int64(values) = &this.values else {
                unreachable!("to_pairs checked integer backing");
            };
            values
                .get(this.values_window())
                .expect("Groups values window is bounded by its backing column")
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
                    // The normalized window is bounded by this admitted row range.
                    this.slice_rows(
                        this.rows.start.wrapping_add(window.start)
                            ..this.rows.start.wrapping_add(window.end),
                    )
                } else {
                    let rows = slice_row_indices(start, step, count);
                    let offsets = this.visible_offsets();
                    match &this.values {
                        GroupsValues::Int64(values) => {
                            let value_count = rows.clone().try_fold(0_usize, |total, row| {
                                total
                                    .checked_add(Self::row_values_window_at(offsets, row).len())
                                    .ok_or_else(|| {
                                        PyOverflowError::new_err(
                                            "Groups values size overflows usize",
                                        )
                                    })
                            })?;
                            let mut builder = CsrBuilder::<i64>::with_capacity(value_count, count);
                            for row in rows {
                                let row_values = values
                                    .get(Self::row_values_window_at(offsets, row))
                                    .expect("Groups row values are bounded by admitted offsets");
                                builder.values_mut().extend_from_slice(row_values);
                                let end = builder.len();
                                builder.push_end(end).map_err(PyErr::from)?;
                            }
                            let (ids, offsets) = builder.finish();
                            Self {
                                values: GroupsValues::Int64(ids.into()),
                                offsets: offsets.into(),
                                rows: 0..count,
                            }
                        },
                        GroupsValues::Geometry(values) => {
                            debug_assert!(
                                !values.has_missing(),
                                "Groups geometry values are compact present rows, never masked"
                            );
                            let value_count = rows.clone().try_fold(0_usize, |total, row| {
                                total
                                    .checked_add(Self::row_values_window_at(offsets, row).len())
                                    .ok_or_else(|| {
                                        PyOverflowError::new_err(
                                            "Groups values size overflows usize",
                                        )
                                    })
                            })?;
                            let mut builder =
                                CsrBuilder::<Shape>::with_capacity(value_count, count);
                            for row in rows {
                                let row_values = values.gather_logical_row_range(
                                    Self::row_values_window_at(offsets, row),
                                )?;
                                for shape in row_values.storage().iter_shapes() {
                                    let shape = shape.map_err(PyErr::from)?.into_owned();
                                    builder.values_mut().push(shape);
                                }
                                let end = builder.len();
                                builder.push_end(end).map_err(PyErr::from)?;
                            }
                            let (shapes, offsets) = builder.finish();
                            let flat = PyGeometryArray::from_shapes(shapes, values.frame.clone())?;
                            Self::from_geometry_csr(flat, offsets)?
                        },
                        GroupsValues::Cells(values) => {
                            let kind = values.kind();
                            let mut builder = CsrBuilder::<u64>::with_capacity(0, count);
                            for row in rows {
                                let row_values = values
                                    .logical_row_range(Self::row_values_window_at(offsets, row));
                                builder
                                    .push_row(row_values.logical_ids())
                                    .map_err(PyErr::from)?;
                            }
                            let (ids, offsets) = builder.finish();
                            Self::from_cell_flat(kind, ids, offsets)?
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
    fn __contains__(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> PyResult<bool> {
        let needle = self.admit_contains_needle(item)?;
        let groups = self.clone();
        py.detach(move || groups.contains_needle(needle))
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
    fn index(
        &self,
        value: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = crate::strict_i64)] start: i64,
        #[pyo3(from_py_with = crate::strict_optional_i64)] stop: Option<i64>,
    ) -> PyResult<usize> {
        let row_len = self.rows.len();
        let len = i64::try_from(row_len).expect("Groups length fits in i64");
        let clamp = |bound: i64| -> usize {
            let resolved = if bound < 0 {
                bound.saturating_add(len)
            } else {
                bound
            };
            usize::try_from(resolved.max(0)).unwrap_or(0).min(row_len)
        };
        let start = clamp(start);
        let stop = stop.map_or(row_len, clamp);
        if start < stop {
            if matches!(&self.values, GroupsValues::Int64(_)) {
                if let Some(expected) = Self::parse_int64_row_item(value)? {
                    for row in start..stop {
                        if self.int64_row_matches(row, &expected) {
                            return Ok(row);
                        }
                    }
                }
            } else {
                for row in start..stop {
                    if self.row_matches(row, value)? {
                        return Ok(row);
                    }
                }
            }
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
    fn count(&self, value: &Bound<'_, PyAny>) -> PyResult<usize> {
        if matches!(&self.values, GroupsValues::Int64(_)) {
            let Some(expected) = Self::parse_int64_row_item(value)? else {
                return Ok(0);
            };
            return Ok((0..self.rows.len())
                .filter(|&row| self.int64_row_matches(row, &expected))
                .count());
        }
        let mut count = 0_usize;
        for row in 0..self.rows.len() {
            count = count.wrapping_add(usize::from(self.row_matches(row, value)?));
        }
        Ok(count)
    }

    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if let Ok(groups) = other.cast::<Self>() {
            let groups = groups.get();
            return self.groups_equal(groups)?.into_py_any(py);
        }
        let Ok(sequence) = other.cast::<PySequence>() else {
            return Ok(py.NotImplemented());
        };
        // Positional, row-aligned: row `i` must equal `sequence[i]` (NOT mere
        // membership — order and multiplicity are significant, as for any
        // Python sequence equality). Do not use `__len__` as a semantic gate:
        // a provider's size hint is advisory and may lie.
        let mut items = sequence.try_iter()?;
        for row in 0..self.rows.len() {
            crate::check_python_signals(py, row)?;
            let Some(item) = items.next() else {
                return false.into_py_any(py);
            };
            let item = item?;
            if !self.row_matches(row, &item)? {
                return false.into_py_any(py);
            }
        }
        match items.next() {
            None => {},
            Some(Err(error)) => return Err(error),
            Some(Ok(_)) => return false.into_py_any(py),
        }
        true.into_py_any(py)
    }

    fn __repr__(&self) -> PyResult<String> {
        match &self.values {
            GroupsValues::Int64(_) => preview(
                (0..self.rows.len()).map(|row| Ok(format!("{:?}", self.int64_row_slice(row)))),
                self.rows.len(),
                "Groups",
            ),
            GroupsValues::Geometry(_) => preview(
                (0..self.rows.len()).map(|row| self.geometry_row_view(row)?.__repr__()),
                self.rows.len(),
                "Groups",
            ),
            GroupsValues::Cells(_) => preview(
                (0..self.rows.len()).map(|row| Ok(self.cell_row_view(row).__repr__())),
                self.rows.len(),
                "Groups",
            ),
        }
    }

    /// Copy into a plain nested Python list.
    ///
    /// Returns
    /// -------
    /// list of list
    ///     Materialized rows of the grouped values.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> groups = gm.GeometryArray([gm.box(0, 0, 2, 2)]).triangulate(method='earcut')
    /// >>> [g.to_wkt() for g in groups.to_list()[0]]
    /// ['POLYGON ((0 2, 0 0, 2 0, 0 2))', 'POLYGON ((2 2, 0 2, 2 0, 2 2))']
    fn to_list(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut rows = Vec::with_capacity(self.rows.len());
        for row in 0..self.rows.len() {
            let value = match &self.values {
                GroupsValues::Int64(_) => {
                    let source = self.int64_row_slice(row);
                    let values = source.to_vec();
                    values.into_py_any(py)?
                },
                GroupsValues::Geometry(_) => {
                    let items = self.geometry_row_view(row)?.materialized_objects(py)?;
                    PyList::new(py, items)?.into_py_any(py)?
                },
                GroupsValues::Cells(_) => {
                    let items = self.cell_row_view(row).to_cell_list(py)?;
                    PyList::new(py, items)?.into_py_any(py)?
                },
            };
            rows.push(value);
        }
        PyList::new(py, rows)?.into_py_any(py)
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let module = crate::gometry_lib_module(py)?;
        // Pickle the LOGICAL groups: rebase offsets to start at 0 and slice the
        // backing to the window, so a sliced Groups round-trips (the
        // `_unpickle_*` validators require offsets[0]==0 and last==values_len).
        let source_offsets = self
            .offsets
            .get(self.rows.start..=self.rows.end)
            .expect("Groups rows are bounded by their admitted offsets");
        let base = *source_offsets
            .first()
            .expect("Groups rows include their starting offset");
        let start = base as usize;
        let end = *source_offsets
            .last()
            .expect("Groups rows include their terminal offset") as usize;
        let mut rebased = Vec::with_capacity(self.rows.len().saturating_add(1));
        for &offset in source_offsets {
            rebased.push(offset.wrapping_sub(base));
        }
        match &self.values {
            GroupsValues::Int64(values) => {
                let copied = values
                    .get(start..end)
                    .expect("Groups offsets are bounded by their int64 values")
                    .to_vec();
                let args = (copied, rebased).into_py_any(py)?;
                Ok((
                    module
                        .getattr(pyo3::intern!(py, "_unpickle_int64_groups_v1"))?
                        .unbind(),
                    args,
                ))
            },
            GroupsValues::Geometry(values) => {
                let windowed = if start == 0 && end == values.len() {
                    values.as_ref().clone()
                } else {
                    values.gather_logical_row_range(start..end)?
                };
                let args = (Py::new(py, windowed)?, rebased).into_py_any(py)?;
                Ok((
                    module
                        .getattr(pyo3::intern!(py, "_unpickle_geometry_groups_v1"))?
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
                        .getattr(pyo3::intern!(py, "_unpickle_cell_groups_v1"))?
                        .unbind(),
                    args,
                ))
            },
        }
    }
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
#[pyfunction]
pub(crate) fn _unpickle_int64_groups_v1(
    ids: &Bound<'_, PyAny>,
    offsets: &Bound<'_, PyAny>,
) -> PyResult<Groups> {
    let ids = crate::collect_i64_sequence(ids, "Groups pickle ids")?;
    let offsets = crate::collect_i64_sequence(offsets, "Groups pickle offsets")?;
    Groups::from_int64_values_csr(ids, offsets)
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
#[pyfunction]
pub(crate) fn _unpickle_geometry_groups_v1(
    values: Py<PyGeometryArray>,
    offsets: &Bound<'_, PyAny>,
) -> PyResult<Groups> {
    let offsets = crate::collect_i64_sequence(offsets, "geometry Groups pickle offsets")?;
    Python::attach(|py| {
        let array = values.bind(py).borrow().clone();
        Groups::from_geometry_csr(array, offsets)
    })
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
#[pyfunction]
pub(crate) fn _unpickle_cell_groups_v1(
    values: Py<PyCellArray>,
    offsets: &Bound<'_, PyAny>,
) -> PyResult<Groups> {
    let offsets = crate::collect_i64_sequence(offsets, "cell Groups pickle offsets")?;
    Python::attach(|py| {
        let array = values.bind(py).borrow().clone();
        Groups::from_cell_csr(array, offsets)
    })
}

/// Internal zero-copy buffer holder for `f64` coordinate columns (Arrow
/// export). Not part of the public surface — constructed from Rust only
/// and consumed through PEP-3118 (`pa.py_buffer`).
#[pyclass(name = "_Float64Buffer", module = "gometry", frozen, immutable_type)]
pub(crate) struct Float64Buffer {
    storage: Arc<[f64]>,
    range: Range<usize>,
}

impl Float64Buffer {
    fn as_slice(&self) -> &[f64] {
        self.storage
            .get(self.range.clone())
            .expect("float64 buffer range is bounded by its immutable storage")
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
#[pyclass(name = "_Int32Buffer", module = "gometry", frozen, immutable_type)]
pub(crate) struct Int32Buffer {
    storage: Arc<[i32]>,
}

impl Int32Buffer {
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
    module.add_function(wrap_pyfunction!(_unpickle_int64_groups_v1, module)?)?;
    module.add_function(wrap_pyfunction!(_unpickle_geometry_groups_v1, module)?)?;
    module.add_function(wrap_pyfunction!(_unpickle_cell_groups_v1, module)?)?;
    Ok(())
}
