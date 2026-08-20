#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Shared row-container iteration and indexing helpers.

use std::sync::atomic::{AtomicUsize, Ordering};

use pyo3::exceptions::{PyIndexError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PySequence, PySlice};

use crate::HeapSize;
use crate::array::{
    CollectedFancyIndex, NumpyFancyIndex, classify_and_collect_fancy_index, is_bool_scalar,
    numpy_fancy_index,
};

/// Atomic row cursor shared by sequence-like Python iterators.
pub(crate) struct RowIterState {
    index: AtomicUsize,
    reverse: bool,
}

impl RowIterState {
    pub(crate) const fn new(reverse: bool) -> Self {
        Self {
            index: AtomicUsize::new(0),
            reverse,
        }
    }

    pub(crate) fn length_hint(&self, len: usize) -> usize {
        len.saturating_sub(self.index.load(Ordering::Relaxed))
    }

    pub(crate) fn next_row(&self, len: usize) -> Option<usize> {
        let position = self.index.fetch_add(1, Ordering::Relaxed);
        if position >= len {
            return None;
        }
        Some(if self.reverse {
            len - 1 - position
        } else {
            position
        })
    }
}

/// Monomorphized row behavior for sequence-like containers.
pub(crate) trait RowContainer: HeapSize + Sized {
    const LABEL: &'static str;
    const INDEX_LABEL: &'static str = Self::LABEL;

    fn row_count(&self) -> usize;
    fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>>;
}

/// Row containers whose Python indexing returns the same container for
/// slice/fancy-index results.
pub(crate) trait RowGetItemContainer: RowContainer {
    fn gather_rows(&self, rows: &[usize]) -> Self;
    fn slice_rows(&self, start: isize, stop: isize, step: isize) -> Self;
    fn empty(&self) -> Self;
    fn container_to_py(py: Python<'_>, value: Self) -> PyResult<Py<PyAny>>;

    fn mask_length_error(mask_len: usize, len: usize, _numpy: bool) -> PyErr {
        PyIndexError::new_err(format!(
            "boolean mask length {mask_len} does not match {} length {len}",
            Self::LABEL,
        ))
    }
}

pub(crate) enum RowIndexOrSlice {
    Index(usize),
    Slice {
        start: isize,
        stop: isize,
        step: isize,
        count: usize,
    },
}

pub(crate) fn normalize_row(index: isize, len: usize, label: &str) -> PyResult<usize> {
    let len = len as isize;
    let normalized = if index < 0 { len + index } else { index };
    usize::try_from(normalized)
        .ok()
        .filter(|&row| row < len as usize)
        .ok_or_else(|| PyIndexError::new_err(format!("{label} index out of range")))
}

pub(crate) fn parse_row_index_or_slice(
    index: &Bound<'_, PyAny>,
    len: usize,
    label: &str,
) -> PyResult<RowIndexOrSlice> {
    if let Ok(slice) = index.cast::<PySlice>() {
        let indices = slice.indices(len as isize)?;
        return Ok(RowIndexOrSlice::Slice {
            start: indices.start,
            stop: indices.stop,
            step: indices.step,
            count: indices.slicelength,
        });
    }
    Ok(RowIndexOrSlice::Index(normalize_row(
        index.extract()?,
        len,
        label,
    )?))
}

pub(crate) fn rows_from_indices(
    indices: impl IntoIterator<Item = isize>,
    len: usize,
    label: &str,
) -> PyResult<Vec<usize>> {
    indices
        .into_iter()
        .map(|index| normalize_row(index, len, label))
        .collect()
}

pub(crate) fn collect_slice_rows(start: isize, stop: isize, step: isize) -> Vec<usize> {
    let mut rows = Vec::new();
    let mut i = start;
    while (step > 0 && i < stop) || (step < 0 && i > stop) {
        rows.push(i as usize);
        i += step;
    }
    rows
}

fn mask_rows<C: RowGetItemContainer>(
    mask: Vec<bool>,
    len: usize,
    numpy: bool,
) -> PyResult<Vec<usize>> {
    if mask.len() != len {
        return Err(C::mask_length_error(mask.len(), len, numpy));
    }
    Ok(mask
        .into_iter()
        .enumerate()
        .filter_map(|(row, keep)| keep.then_some(row))
        .collect())
}

fn index_type_error(label: &str) -> PyErr {
    PyTypeError::new_err(format!(
        "{label} indices must be integers, slices, integer sequences, or boolean masks"
    ))
}

fn bool_scalar_error(label: &str) -> PyErr {
    PyTypeError::new_err(format!(
        "boolean scalar is not a {label} index; use an integer index or a boolean mask"
    ))
}

pub(crate) fn array_getitem<C: RowGetItemContainer>(
    container: &C,
    py: Python<'_>,
    index: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let len = container.row_count();
    if let Ok(slice) = index.cast::<pyo3::types::PySlice>() {
        let indices = slice.indices(len as isize)?;
        return C::container_to_py(
            py,
            container.slice_rows(indices.start, indices.stop, indices.step),
        );
    }
    if let Some(index) = numpy_fancy_index(index, C::INDEX_LABEL, C::LABEL)? {
        return match index {
            NumpyFancyIndex::Scalar(index) => {
                container.scalar_row(py, normalize_row(index, len, C::INDEX_LABEL)?)
            },
            NumpyFancyIndex::Mask(mask) => {
                C::container_to_py(py, container.gather_rows(&mask_rows::<C>(mask, len, true)?))
            },
            NumpyFancyIndex::Indices(indices) => C::container_to_py(
                py,
                container.gather_rows(&rows_from_indices(indices, len, C::INDEX_LABEL)?),
            ),
        };
    }
    if let Ok(sequence) = index.cast::<PySequence>() {
        // One-pass classify + collect: never classify with one `__len__` and
        // then `Vec<T>: FromPyObject` with another (lying sequences can report
        // len 1 at classify and sys.maxsize at extract → allocator abort).
        match classify_and_collect_fancy_index(py, sequence)? {
            CollectedFancyIndex::Empty => return C::container_to_py(py, container.empty()),
            CollectedFancyIndex::Indices(indices) => {
                return C::container_to_py(
                    py,
                    container.gather_rows(&rows_from_indices(indices, len, C::INDEX_LABEL)?),
                );
            },
            CollectedFancyIndex::Mask(mask) => {
                return C::container_to_py(
                    py,
                    container.gather_rows(&mask_rows::<C>(mask, len, false)?),
                );
            },
            CollectedFancyIndex::Invalid => {},
        }
        return Err(index_type_error(C::INDEX_LABEL));
    }
    if is_bool_scalar(index) {
        return Err(bool_scalar_error(C::LABEL));
    }
    let index: isize = index.extract()?;
    container.scalar_row(py, normalize_row(index, len, C::INDEX_LABEL)?)
}
