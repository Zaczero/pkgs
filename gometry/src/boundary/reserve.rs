//! Checked layouts and bounded reservations for external counts and hints.
//!
//! Public Arrow lengths, Python `__len__` values, and multi-chunk coordinate
//! totals are attacker-controlled. This module rejects arithmetic and Rust
//! layout overflow before allocation; allocator exhaustion itself follows the
//! process-fatal native contract.
//!
//! ## Hints vs validated totals
//!
//! - **Hints** (`reserve_hint`): Python `__len__` and similar presizing
//!   guesses. Oversized hints are **clamped** (never a rejection that changes
//!   validity).
//! - **Validated totals** (`try_reserve_total` / `try_vec_with_capacity`):
//!   checked sums of Arrow lengths and other exact, already-validated counts.
//!   These use **uncapped** checked-layout reservation so multi-chunk import
//!   cannot reject a total that single-chunk would accept.
//!
//! ## No artificial element-count caps
//!
//! Valid large finite inputs (lists, generators of the same length) must share
//! one outcome: success or process-fatal native allocator exhaustion.
//! Never reject with "too large to materialize" or a bare-stream ceiling; that
//! invents a list-vs-generator asymmetry. The collector's retained output
//! grows proportionally (see [`collect_py_iter`]).

use std::collections::BinaryHeap;

use pyo3::exceptions::{PyMemoryError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyIterator, PySequence};

use crate::error::{ResourceError, Result};

/// Soft upper bound for a single **initial** reservation from an external
/// length *hint* (`__len__`, etc.). Large enough to avoid thrash on typical
/// batch sizes; small enough that a lying `sys.maxsize` / `i64::MAX` hint
/// cannot force a multi-gigabyte empty `Vec` before the first item is
/// yielded. Never promote a hint to an exact allocation or iteration count.
pub(crate) const MAX_RESERVE_HINT: usize = 1 << 12; // 4096 elements

/// Check pending Python signals often enough that a native loop over a Python
/// iterator remains interruptible without paying for a signal check on every
/// item.
pub(crate) const PYTHON_SIGNAL_STRIDE: usize = 64;

pub(crate) fn check_python_signals(py: Python<'_>, item: usize) -> PyResult<()> {
    if item.is_multiple_of(PYTHON_SIGNAL_STRIDE) {
        py.check_signals()?;
    }
    Ok(())
}

/// Acquire a Python iterator while distinguishing a plain non-iterable from
/// an iterable provider whose protocol raised. The latter exception is part
/// of the caller's input and must not be rewritten as a type classification.
pub(crate) fn py_iter_or_error<'py>(
    value: &Bound<'py, PyAny>,
    error: impl FnOnce() -> PyErr,
) -> PyResult<Bound<'py, PyIterator>> {
    match value.try_iter() {
        Ok(iter) => Ok(iter),
        Err(protocol_error)
            if protocol_error.is_instance_of::<PyTypeError>(value.py())
                && value
                    .getattr_opt(pyo3::intern!(value.py(), "__iter__"))?
                    .is_none()
                && value
                    .getattr_opt(pyo3::intern!(value.py(), "__getitem__"))?
                    .is_none() =>
        {
            Err(error())
        },
        Err(protocol_error) => Err(protocol_error),
    }
}

/// Read a Python size only when the object actually exposes ``__len__``.
/// Missing length is normal for a one-shot iterator; an error from a supplied
/// ``__len__`` belongs to the provider and must not be demoted to "no hint".
pub(crate) fn advisory_len(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    let Some(len) = value.getattr_opt(pyo3::intern!(value.py(), "__len__"))? else {
        return Ok(None);
    };
    if !len.is_callable() {
        return Err(PyTypeError::new_err("__len__ must be callable"));
    }
    value.len().map(Some)
}

fn check_vec_capacity<T>(len: usize, additional: usize) -> Result<()> {
    let capacity = len
        .checked_add(additional)
        .ok_or(ResourceError::SizeOverflow)?;
    std::alloc::Layout::array::<T>(capacity).map_err(|_| ResourceError::SizeOverflow)?;
    Ok(())
}

/// Reserve output storage after rejecting an impossible element layout.
/// Allocator exhaustion follows Rust's process-fatal native contract.
pub(crate) fn try_reserve_vec<T>(vec: &mut Vec<T>, additional: usize) -> Result<()> {
    check_vec_capacity::<T>(vec.len(), additional)?;
    vec.reserve(additional);
    Ok(())
}

/// Append one value after rejecting the only recoverable failure: an
/// impossible element layout.
pub(crate) fn try_push_vec<T>(vec: &mut Vec<T>, value: T) -> Result<()> {
    if vec.len() == vec.capacity() {
        std::hint::cold_path();
        check_vec_capacity::<T>(vec.len(), 1)?;
    }
    vec.push(value);
    Ok(())
}

/// Reserve two parallel output columns after checking both layouts.
pub(crate) fn try_reserve_pair<T, U>(
    left: &mut Vec<T>,
    right: &mut Vec<U>,
    additional: usize,
) -> Result<()> {
    check_vec_capacity::<T>(left.len(), additional)?;
    check_vec_capacity::<U>(right.len(), additional)?;
    left.reserve(additional);
    right.reserve(additional);
    Ok(())
}

/// Append one item to each parallel column after both resulting layouts fit.
pub(crate) fn try_push_pair<T, U>(
    left: &mut Vec<T>,
    right: &mut Vec<U>,
    left_value: T,
    right_value: U,
) -> Result<()> {
    if left.len() == left.capacity() {
        check_vec_capacity::<T>(left.len(), 1)?;
    }
    if right.len() == right.capacity() {
        check_vec_capacity::<U>(right.len(), 1)?;
    }
    left.push(left_value);
    right.push(right_value);
    Ok(())
}

/// Append one ordered work item after checking the heap element layout.
pub(crate) fn try_push_heap<T: Ord>(heap: &mut BinaryHeap<T>, value: T) -> Result<()> {
    if heap.len() == heap.capacity() {
        check_vec_capacity::<T>(heap.len(), 1)?;
    }
    heap.push(value);
    Ok(())
}

/// Reserve a known heap capacity after checking its element layout.
pub(crate) fn try_reserve_heap<T>(heap: &mut BinaryHeap<T>, additional: usize) -> Result<()> {
    check_vec_capacity::<T>(heap.len(), additional)?;
    heap.reserve(additional);
    Ok(())
}

/// One generic CSR owner for ragged output. Every row end is monotonic and
/// representable as `i64` before it is retained. Empty offsets mean the
/// implicit initial zero; the finished zero-row representation is `[0]`.
pub(crate) struct CsrBuilder<T> {
    values: Vec<T>,
    offsets: Vec<i64>,
}

impl<T> CsrBuilder<T> {
    pub(crate) const fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Reserve exact, already-validated capacities for values and rows.
    pub(crate) fn with_capacity(value_capacity: usize, row_capacity: usize) -> Result<Self> {
        let rows = row_capacity
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        std::alloc::Layout::array::<T>(value_capacity).map_err(|_| ResourceError::SizeOverflow)?;
        std::alloc::Layout::array::<i64>(rows).map_err(|_| ResourceError::SizeOverflow)?;
        Ok(Self {
            values: Vec::with_capacity(value_capacity),
            offsets: Vec::with_capacity(rows),
        })
    }

    /// Reserve only a bounded initial chunk from advisory external hints.
    /// Hints never affect validity; observed values retain ordinary growth.
    #[cfg(test)]
    pub(crate) fn with_capacity_hint(value_hint: usize, row_hint: usize) -> Self {
        Self {
            values: Vec::with_capacity(value_hint.min(MAX_RESERVE_HINT)),
            offsets: Vec::with_capacity(row_hint.min(MAX_RESERVE_HINT) + 1),
        }
    }

    fn release(&mut self) {
        self.values = Vec::new();
        self.offsets = Vec::new();
    }

    pub(crate) fn push_row(&mut self, values: impl IntoIterator<Item = T>) -> Result<()> {
        let values = values.into_iter();
        self.values.reserve(values.size_hint().0);
        self.values.extend(values);
        let end = self.values.len();
        self.push_end(end)
    }

    /// Append a checked row end after values have been emitted. This is also
    /// used by packed representations whose physical value count differs from
    /// the logical row count (for example, four vertices per triangle).
    pub(crate) fn push_end(&mut self, end: usize) -> Result<()> {
        let Ok(end) = i64::try_from(end) else {
            self.release();
            return Err(ResourceError::SizeOverflow.into());
        };
        let last = self.offsets.last().copied().unwrap_or(0);
        if end < last {
            self.release();
            return Err(crate::geometry::GeometryErrorKind::MalformedCsrOffsets.into());
        }
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
        self.offsets.push(end);
        Ok(())
    }

    pub(crate) const fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) const fn row_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub(crate) fn clear(&mut self) {
        self.release();
    }

    pub(crate) const fn values_mut(&mut self) -> &mut Vec<T> {
        &mut self.values
    }

    #[cfg(test)]
    pub(crate) fn row_range(&self, row: usize) -> std::ops::Range<usize> {
        self.offsets[row] as usize..self.offsets[row + 1] as usize
    }

    pub(crate) fn finish(self) -> (Vec<T>, Vec<i64>) {
        let Self {
            values,
            mut offsets,
        } = self;
        if offsets.is_empty() {
            offsets.push(0);
        }
        (values, offsets)
    }
}

/// Reserve from an untrusted **hint** without letting the hint affect
/// validity. The reservation is clamped to both the advisory bound and `T`'s
/// remaining addressable layout.
pub(crate) fn reserve_hint<T>(vec: &mut Vec<T>, additional: usize) {
    let element_size = std::mem::size_of::<T>();
    let max_capacity = (isize::MAX as usize)
        .checked_div(element_size)
        .unwrap_or(usize::MAX);
    let additional = additional
        .min(MAX_RESERVE_HINT)
        .min(max_capacity.saturating_sub(vec.len()));
    vec.reserve(additional);
}

/// Reserve capacity for an exact external total after checking Rust's layout.
pub(crate) fn try_reserve_total<T>(vec: &mut Vec<T>, additional: usize) -> PyResult<()> {
    check_vec_capacity::<T>(vec.len(), additional).map_err(PyErr::from)?;
    vec.reserve(additional);
    Ok(())
}

/// Build an empty `Vec` from a validated external total.
pub(crate) fn try_vec_with_capacity<T>(capacity: usize) -> PyResult<Vec<T>> {
    let mut vec = Vec::new();
    try_reserve_total(&mut vec, capacity)?;
    Ok(vec)
}

/// Build an empty `Vec` from an untrusted length hint (clamped).
pub(crate) fn vec_with_capacity_hint<T>(capacity: usize) -> Vec<T> {
    let mut vec = Vec::new();
    reserve_hint(&mut vec, capacity);
    vec
}

/// Push one element while retaining a typed layout-overflow error.
pub(crate) fn try_push<T>(vec: &mut Vec<T>, item: T) -> PyResult<()> {
    try_push_vec(vec, item).map_err(PyErr::from)
}

/// Collect a fixed-length bool mask without generic `Vec<bool>: FromPyObject`
/// (which allocates from a lying `__len__` before any Rust length check).
///
/// Accepts any sequence of `expected_len` bools; length is taken from the
/// array, not from the mask's `__len__` as a capacity hint for bulk extract.
pub(crate) fn collect_bool_mask(
    _py: Python<'_>,
    mask: &Bound<'_, PyAny>,
    expected_len: usize,
) -> PyResult<Vec<bool>> {
    if let Some(reported_len) = advisory_len(mask)?
        && reported_len != expected_len
    {
        return Err(PyValueError::new_err(format!(
            "mask length {reported_len} does not match array length {expected_len}"
        )));
    }
    // The iterator is the source of truth even for sequences: ``__len__`` is
    // only a reservation hint and a lying sequence must not lose or invent
    // rows. Exact collection reads one extra item to detect excess.
    collect_py_iter_exact(
        mask,
        expected_len,
        |item| {
            item.extract::<bool>()
                .map_err(|_| PyTypeError::new_err("mask elements must be bool"))
        },
        |got| {
            PyValueError::new_err(format!(
                "mask length {got} does not match array length {expected_len}"
            ))
        },
    )
    .map_err(|error| {
        if error.is_instance_of::<pyo3::exceptions::PyTypeError>(mask.py())
            && error.to_string().contains("not iterable")
        {
            PyTypeError::new_err("mask must be a sequence or iterable of bool")
        } else {
            error
        }
    })
}

/// Collect sequence elements once (classify + materialize in one pass) without
/// trusting a reported length. Never uses generic `Vec<T>: FromPyObject`.
pub(crate) fn collect_sequence_items(sequence: &Bound<'_, PySequence>) -> PyResult<Vec<Py<PyAny>>> {
    // Prefer the iterator protocol so a lying `__len__` cannot force a huge
    // empty allocation; observed elements drive real growth.
    let any = sequence.as_any();
    match any.try_iter() {
        Ok(iter) => {
            let mut out = Vec::new();
            for (index, item) in iter.enumerate() {
                check_python_signals(any.py(), index)?;
                try_push(&mut out, item?.unbind())?;
            }
            return Ok(out);
        },
        Err(err)
            if err.is_instance_of::<pyo3::exceptions::PyTypeError>(any.py())
                && any
                    .getattr_opt(pyo3::intern!(any.py(), "__iter__"))?
                    .is_none() => {},
        Err(err) => return Err(err),
    }
    // Legacy getitem-only sequences have no iterator attribute. Walk until
    // their protocol raises IndexError; the reported length remains advisory.
    let mut out = Vec::new();
    let mut index = 0;
    loop {
        check_python_signals(any.py(), index)?;
        let item = match sequence.get_item(index) {
            Ok(item) => item,
            Err(error) if error.is_instance_of::<pyo3::exceptions::PyIndexError>(any.py()) => break,
            Err(error) => return Err(error),
        };
        try_push(&mut out, item.unbind())?;
        index = index.saturating_add(1);
    }
    Ok(out)
}

/// Collect a sequence of concrete byte payloads (pickle mixed rows). Each
/// element must be `bytes` / a buffer; never `Vec<u8>` from a generic sequence.
pub(crate) fn collect_bytes_rows(rows: &Bound<'_, PyAny>) -> PyResult<Vec<crate::OwnedBytes>> {
    let sequence = rows.cast::<PySequence>().map_err(|_| {
        PyTypeError::new_err("mixed geometry array pickle rows must be a sequence of bytes")
    })?;
    let any = sequence.as_any();
    let mut out = Vec::new();
    match any.try_iter() {
        Ok(iter) => {
            for (index, item) in iter.enumerate() {
                check_python_signals(any.py(), index)?;
                let item = item?;
                let bytes = extract_bytes_payload(&item)?;
                try_push(&mut out, bytes)?;
            }
            return Ok(out);
        },
        Err(err)
            if err.is_instance_of::<pyo3::exceptions::PyTypeError>(any.py())
                && any
                    .getattr_opt(pyo3::intern!(any.py(), "__iter__"))?
                    .is_none() => {},
        Err(err) => return Err(err),
    }
    let mut index = 0;
    loop {
        check_python_signals(any.py(), index)?;
        let item = match sequence.get_item(index) {
            Ok(item) => item,
            Err(error) if error.is_instance_of::<pyo3::exceptions::PyIndexError>(any.py()) => break,
            Err(error) => return Err(error),
        };
        try_push(&mut out, extract_bytes_payload(&item)?)?;
        index = index.saturating_add(1);
    }
    Ok(out)
}

fn extract_bytes_payload(item: &Bound<'_, PyAny>) -> PyResult<crate::OwnedBytes> {
    if let Some(bytes) = crate::exact_backed_bytes(item) {
        return Ok(crate::OwnedBytes::Backed(bytes));
    }
    // Shared one-byte buffer path (signed + unsigned); never generic
    // sequence-of-int extract (that path allocates from a lying `__len__`).
    copy_one_byte_buffer(item)
        .map(crate::OwnedBytes::Copied)
        .map_err(|error| {
            if error.is_instance_of::<PyMemoryError>(item.py()) {
                error
            } else {
                PyTypeError::new_err("mixed geometry array pickle rows must be bytes-like")
            }
        })
}

/// Copy a one-byte Python buffer into exclusive Rust storage. This is the
/// owning counterpart to [`with_one_byte_buffer`].
/// The source producer must remain quiescent until the copy returns; owning
/// the result does not synchronize the source read.
pub(crate) fn copy_one_byte_buffer(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = value.extract::<&[u8]>() {
        let mut owned = try_vec_with_capacity(bytes.len())?;
        owned.extend_from_slice(bytes);
        return Ok(owned);
    }
    if let Ok(buffer) = pyo3::buffer::PyBuffer::<u8>::get(value) {
        if buffer.item_size() != 1 {
            return Err(PyTypeError::new_err(
                "expected a one-byte buffer (itemsize 1)",
            ));
        }
        let mut owned = try_vec_with_capacity(buffer.item_count())?;
        owned.resize(buffer.item_count(), 0);
        buffer.copy_to_slice(value.py(), &mut owned)?;
        return Ok(owned);
    }
    if let Ok(buffer) = pyo3::buffer::PyBuffer::<i8>::get(value) {
        if buffer.item_size() != 1 {
            return Err(PyTypeError::new_err(
                "expected a one-byte buffer (itemsize 1)",
            ));
        }
        let mut signed = try_vec_with_capacity(buffer.item_count())?;
        signed.resize(buffer.item_count(), 0);
        buffer.copy_to_slice(value.py(), &mut signed)?;
        let (ptr, len, capacity) = signed.into_raw_parts();
        // SAFETY: i8 and u8 have the same layout and every byte is initialized.
        return Ok(unsafe { Vec::from_raw_parts(ptr.cast(), len, capacity) });
    }
    Err(PyTypeError::new_err(
        "expected a one-byte buffer (bytes, bytearray, memoryview, or array.array)",
    ))
}

/// Collect Python strings while preserving provider and type errors.
pub(crate) fn collect_py_strings(values: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<String>> {
    let mut out = Vec::new();
    if let Some(hint) = advisory_len(values)? {
        reserve_hint(&mut out, hint);
    }
    let iter = py_iter_or_error(values, || {
        PyTypeError::new_err(format!("{what} must be an iterable of strings"))
    })?;
    for (index, item) in iter.enumerate() {
        let item = item?;
        check_python_signals(values.py(), index)?;
        let text: &str = item
            .extract()
            .map_err(|_| PyTypeError::new_err(format!("{what} elements have the wrong type")))?;
        let owned = text.to_owned();
        try_push(&mut out, owned)?;
    }
    Ok(out)
}

/// True when ``value`` exports a one-byte buffer (signed or unsigned item
/// format): ``bytes``/``bytearray``/``memoryview``/``array.array('B'|'b')``.
pub(crate) fn is_one_byte_buffer(value: &Bound<'_, PyAny>) -> bool {
    value.extract::<&[u8]>().is_ok()
        || pyo3::buffer::PyBuffer::<u8>::get(value).is_ok()
        || pyo3::buffer::PyBuffer::<i8>::get(value).is_ok()
}

/// Hand the bytes of any one-byte buffer to ``f``.
///
/// Accepts ``bytes``/``bytearray``/``memoryview`` and other buffer-protocol
/// exporters with itemsize 1 — both unsigned (``'B'``) and signed (``'b'``).
///
/// Free-threading soundness: zero-copy `&[u8]` is allowed **only** for
/// known-immutable ``bytes`` (`extract::<&[u8]>` is PyBytes-only). Mutable
/// carriers (`bytearray`, writable `memoryview`, NumPy, …) are **copied**
/// before parse. A `PyBuffer` pins the allocation but does not stop another
/// thread mutating contents, so the producer must remain quiescent until the
/// copy returns; the resulting parse borrows only the owned copy.
pub(crate) fn with_one_byte_buffer<R>(
    value: &Bound<'_, PyAny>,
    f: impl FnOnce(&[u8]) -> PyResult<R>,
) -> PyResult<R> {
    // Immutable bytes only — PyO3's `&[u8]` extract is PyBytes-gated.
    if let Ok(bytes) = value.extract::<&[u8]>() {
        return f(bytes);
    }
    if let Ok(buffer) = pyo3::buffer::PyBuffer::<u8>::get(value) {
        return with_u8_buffer_owned(&buffer, value.py(), f);
    }
    if let Ok(buffer) = pyo3::buffer::PyBuffer::<i8>::get(value) {
        return with_i8_buffer_owned(&buffer, value.py(), f);
    }
    Err(PyTypeError::new_err(
        "expected a one-byte buffer (bytes, bytearray, memoryview, or array.array)",
    ))
}

/// Copy a `u8` `PyBuffer` into an exclusive `Vec<u8>`, then hand `&[u8]` to ``f``.
///
/// The slice always refers to the Rust-owned `Vec` — never to the Python
/// exporter's memory — so later mutators of the original `bytearray` /
/// writable `memoryview` cannot alias through this borrow. The copy itself is
/// not atomic and does not synchronize a native writer; the producer must
/// remain quiescent until it returns.
fn with_u8_buffer_owned<R>(
    buffer: &pyo3::buffer::PyBuffer<u8>,
    py: Python<'_>,
    f: impl FnOnce(&[u8]) -> PyResult<R>,
) -> PyResult<R> {
    if buffer.item_size() != 1 {
        return Err(PyTypeError::new_err(
            "expected a one-byte buffer (itemsize 1)",
        ));
    }
    // Copy into exclusively owned Rust storage.
    let mut owned = try_vec_with_capacity(buffer.item_count())?;
    owned.resize(buffer.item_count(), 0);
    buffer.copy_to_slice(py, &mut owned)?;
    f(&owned)
}

/// Copy a signed one-byte `PyBuffer` into an exclusive `Vec<u8>`, then parse.
fn with_i8_buffer_owned<R>(
    buffer: &pyo3::buffer::PyBuffer<i8>,
    py: Python<'_>,
    f: impl FnOnce(&[u8]) -> PyResult<R>,
) -> PyResult<R> {
    if buffer.item_size() != 1 {
        return Err(PyTypeError::new_err(
            "expected a one-byte buffer (itemsize 1)",
        ));
    }
    let mut signed = try_vec_with_capacity(buffer.item_count())?;
    signed.resize(buffer.item_count(), 0);
    buffer.copy_to_slice(py, &mut signed)?;
    // Bit-preserving i8 → u8 on exclusive storage (no alias into Python
    // memory). The types have identical layout, so this reuses the one
    // allocated buffer instead of retaining two copies.
    let (ptr, len, capacity) = signed.into_raw_parts();
    // SAFETY: `i8` and `u8` are same-size, same-alignment byte types and every
    // element is initialized by `copy_to_slice` above.
    let owned = unsafe { Vec::from_raw_parts(ptr.cast(), len, capacity) };
    f(&owned)
}

/// Collect `u64` ids for pickle reconstructors (cells, coverages, …).
pub(crate) fn collect_u64_sequence(value: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<u64>> {
    collect_py_iter(value, |item| try_u64_item(&item, what))
}

/// Collect `i64` CSR offsets / int groups for pickle reconstructors.
pub(crate) fn collect_i64_sequence(value: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<i64>> {
    collect_py_iter(value, |item| try_i64_item(&item, what))
}

fn try_u64_item(value: &Bound<'_, PyAny>, what: &str) -> PyResult<u64> {
    match crate::py_u64_required(what, value) {
        Ok(value) => Ok(value),
        Err(error)
            if error.is_instance_of::<PyTypeError>(value.py()) && !crate::py_index_check(value) =>
        {
            Err(PyTypeError::new_err(format!(
                "{what} elements have the wrong type"
            )))
        },
        Err(error) => Err(error),
    }
}

fn try_i64_item(value: &Bound<'_, PyAny>, what: &str) -> PyResult<i64> {
    match crate::py_i64_required(what, value) {
        Ok(value) => Ok(value),
        Err(error)
            if error.is_instance_of::<PyTypeError>(value.py()) && !crate::py_index_check(value) =>
        {
            Err(PyTypeError::new_err(format!(
                "{what} elements have the wrong type"
            )))
        },
        Err(error) => Err(error),
    }
}

/// THE mandatory entry for every Python-iterable → `Vec` boundary.
///
/// `__len__` is a clamped **hint** only. Callers that need a mapped type pass a
/// closure; the identity map (`|item| Ok(item)`) materializes raw `Bound` items.
///
/// There is no bare-stream element ceiling: a finite generator of N elements
/// has the same validity as a list of N. Provider and mapping errors stop the
/// iteration and release retained output; native allocator exhaustion aborts.
pub(crate) fn collect_py_iter<'py, T>(
    values: &Bound<'py, PyAny>,
    map: impl FnMut(Bound<'py, PyAny>) -> PyResult<T>,
) -> PyResult<Vec<T>> {
    let iter = values.try_iter()?;
    collect_py_iter_from_iter(values, iter, map)
}

/// Collect from an iterator that the caller already acquired. Keeping the
/// acquisition outside the collector lets callers distinguish a non-iterable
/// scalar from an iterator provider that failed during acquisition, without a
/// destructive second probe.
pub(crate) fn collect_py_iter_from_iter<'py, T>(
    values: &Bound<'py, PyAny>,
    mut iter: Bound<'py, PyIterator>,
    mut map: impl FnMut(Bound<'py, PyAny>) -> PyResult<T>,
) -> PyResult<Vec<T>> {
    let mut out = Vec::new();
    // Length is a clamped *hint* only — never a validity gate.
    if let Some(hint) = advisory_len(values)? {
        reserve_hint(&mut out, hint);
    }
    let mut index = 0;
    loop {
        let item = match iter.next() {
            None => break,
            Some(Ok(item)) => item,
            Some(Err(err)) => {
                drop(out);
                return Err(err);
            },
        };
        check_python_signals(values.py(), index)?;
        index = index.saturating_add(1);
        // Reject an impossible element layout before mapping; valid-layout
        // native growth follows Rust's process-fatal allocator contract.
        if out.len() == out.capacity() {
            check_vec_capacity::<T>(out.len(), 1).map_err(PyErr::from)?;
            out.reserve(1);
        }
        match map(item) {
            Ok(value) => out.push(value),
            Err(err) => {
                drop(out);
                return Err(err);
            },
        }
    }
    Ok(out)
}

/// Collect **exactly** `expected` items from a Python iterable.
///
/// Stops after `expected + 1` elements so an unbounded iterator (e.g.
/// `itertools.repeat`) cannot grow forever when the length is known from the
/// receiver. The validated count receives one checked-layout reservation.
///
/// On length mismatch, `on_len` receives the observed count (or `expected + 1`
/// when the iterator still has more). Domain callers use this for stable
/// messages (`dissolve`, feature alignment).
pub(crate) fn collect_py_iter_exact<'py, T>(
    values: &Bound<'py, PyAny>,
    expected: usize,
    map: impl FnMut(Bound<'py, PyAny>) -> PyResult<T>,
    on_len: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<T>> {
    let iter = values.try_iter()?;
    collect_py_iter_exact_from_iter(values, iter, expected, map, on_len)
}

/// Exact collector sibling for callers that already acquired the iterator.
pub(crate) fn collect_py_iter_exact_from_iter<'py, T>(
    values: &Bound<'py, PyAny>,
    mut iter: Bound<'py, PyIterator>,
    expected: usize,
    mut map: impl FnMut(Bound<'py, PyAny>) -> PyResult<T>,
    on_len: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<T>> {
    let mut out = try_vec_with_capacity(expected)?;
    for index in 0..expected {
        check_python_signals(values.py(), index)?;
        let Some(item) = iter.next() else {
            break;
        };
        let item = match item {
            Ok(item) => item,
            Err(err) => {
                drop(out);
                return Err(err);
            },
        };
        let value = match map(item) {
            Ok(value) => value,
            Err(err) => {
                drop(out);
                return Err(err);
            },
        };
        try_push(&mut out, value)?;
    }
    if out.len() != expected {
        let got = out.len();
        drop(out);
        return Err(on_len(got));
    }
    // Surplus detection BEFORE mapping: do not run caller code whose result
    // would be discarded (side-effecting maps, expensive decode, etc.).
    match iter.next() {
        None => Ok(out),
        Some(Err(err)) => {
            drop(out);
            Err(err)
        },
        Some(Ok(_extra)) => {
            drop(out);
            Err(on_len(expected.saturating_add(1)))
        },
    }
}

/// Bounded collector sibling for callers that already acquired the iterator.
pub(crate) fn collect_py_iter_at_most_from_iter<'py, T>(
    values: &Bound<'py, PyAny>,
    mut iter: Bound<'py, PyIterator>,
    maximum: usize,
    mut map: impl FnMut(Bound<'py, PyAny>) -> PyResult<T>,
    on_excess: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<T>> {
    let mut out = Vec::new();
    for index in 0..maximum {
        check_python_signals(values.py(), index)?;
        let Some(item) = iter.next() else {
            return Ok(out);
        };
        let item = match item {
            Ok(item) => item,
            Err(err) => {
                drop(out);
                return Err(err);
            },
        };
        let value = match map(item) {
            Ok(value) => value,
            Err(err) => {
                drop(out);
                return Err(err);
            },
        };
        try_push(&mut out, value)?;
    }
    check_python_signals(values.py(), maximum)?;
    match iter.next() {
        None => Ok(out),
        Some(Err(err)) => {
            drop(out);
            Err(err)
        },
        Some(Ok(_)) => {
            drop(out);
            Err(on_excess(maximum.saturating_add(1)))
        },
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;

    #[test]
    fn reserve_hint_clamps_instead_of_rejecting() {
        let mut v: Vec<u8> = Vec::new();
        // Formerly raised ValueError for additional > MAX_RESERVE_HINT.
        reserve_hint(&mut v, MAX_RESERVE_HINT + 1);
        assert!(v.capacity() <= MAX_RESERVE_HINT);
        assert!(v.capacity() > 0);
    }

    #[test]
    fn reserve_total_accepts_above_hint_cap() {
        let mut v: Vec<u8> = Vec::new();
        // Validated totals must not share the hint cap as a rejection.
        // Use u8 so the allocation stays ~64 MiB + 1, not f64-scale.
        try_reserve_total(&mut v, MAX_RESERVE_HINT + 1).expect("small validated total");
        assert!(v.capacity() > MAX_RESERVE_HINT);
    }

    #[test]
    fn try_push_grows_without_trusting_bulk_hints() {
        // A lying bulk hint never becomes an exact allocation.
        let mut v: Vec<u32> = Vec::new();
        for i in 0..64 {
            try_push(&mut v, i).expect("small growth must succeed");
        }
        assert_eq!(v.len(), 64);
        assert!(v.capacity() >= 64);
    }

    #[test]
    fn at_most_collector_drops_retained_values_before_excess_error() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            struct DropProbe {
                id: u8,
                dropped: Rc<RefCell<Vec<u8>>>,
            }

            impl Drop for DropProbe {
                fn drop(&mut self) {
                    self.dropped.borrow_mut().push(self.id);
                }
            }

            let dropped = Rc::new(RefCell::new(Vec::new()));
            let values = pyo3::types::PyList::new(py, [0_u8, 1, 2, 3]).expect("fixture list");
            let iter = values.try_iter().expect("list iterates");
            let result = collect_py_iter_at_most_from_iter(
                values.as_any(),
                iter,
                2,
                |item| {
                    Ok(DropProbe {
                        id: item.extract()?,
                        dropped: Rc::clone(&dropped),
                    })
                },
                |observed| {
                    assert_eq!(observed, 3);
                    let mut ids = dropped.borrow().clone();
                    ids.sort_unstable();
                    assert_eq!(
                        ids,
                        [0, 1],
                        "retained values must already be dropped when on_excess runs"
                    );
                    pyo3::exceptions::PyValueError::new_err("excess")
                },
            );
            assert!(result.is_err(), "one extra item must raise");
        });
    }

    #[test]
    fn at_most_collector_preserves_provider_error_identity_after_cleanup() {
        crate::test_support::initialize_python();
        Python::attach(|py| -> PyResult<()> {
            let _signal_guard = crate::test_support::PythonSignalTestGuard::new(py);
            let sentinel = pyo3::exceptions::PyRuntimeError::new_err("iterator sentinel")
                .value(py)
                .clone()
                .into_any()
                .unbind();
            let item = pyo3::types::PyInt::new(py, 1);
            let iterator = crate::test_support::native_boundary_iterator(
                py,
                item.as_any(),
                4,
                None,
                Some((2, sentinel.clone_ref(py))),
            )?;
            let bound = iterator.bind(py);
            let values = bound.as_any();
            let result = collect_py_iter_at_most_from_iter(
                values,
                values.try_iter()?,
                2,
                |item| item.extract::<i64>(),
                |_| pyo3::exceptions::PyValueError::new_err("excess"),
            );
            let error = result.expect_err("provider sentinel must be returned");
            assert_eq!(error.value(py).as_ptr(), sentinel.as_ptr());
            let state = iterator.bind(py).borrow();
            assert_eq!(state.calls(), 3);
            assert_eq!(state.remaining(), 2);
            Ok(())
        })
        .expect("provider identity after cleanup");
    }

    #[test]
    fn try_push_pair_keeps_parallel_columns_aligned() {
        let mut left = Vec::new();
        let mut right = Vec::new();
        try_push_pair(&mut left, &mut right, 7_u8, 11_u16).expect("small pair must succeed");
        try_push_pair(&mut left, &mut right, 13_u8, 17_u16).expect("small pair must succeed");
        assert_eq!(left, [7, 13]);
        assert_eq!(right, [11, 17]);
    }

    #[test]
    fn try_push_has_no_element_count_ceiling() {
        // Spot-check that ordinary growth has no advisory-hint ceiling.
        let mut v: Vec<u8> = Vec::new();
        for i in 0..10_000 {
            try_push(&mut v, (i & 0xFF) as u8).expect("no artificial count ceiling");
        }
        assert_eq!(v.len(), 10_000);
    }

    #[test]
    fn vec_with_capacity_hint_clamps_absurd_counts() {
        let v: Vec<u8> = vec_with_capacity_hint(usize::MAX / 8);
        assert!(v.capacity() <= MAX_RESERVE_HINT);
    }

    #[test]
    fn csr_builder_preserves_empty_rows_and_checked_offsets() {
        let mut builder = CsrBuilder::<u8>::with_capacity_hint(4, 3);
        builder.push_row(std::iter::empty()).expect("empty row");
        builder.push_row([1, 2]).expect("values");
        builder
            .push_row(std::iter::empty())
            .expect("second empty row");
        assert_eq!(builder.row_range(1), 0..2);
        let (values, offsets) = builder.finish();
        assert_eq!(values, [1, 2]);
        assert_eq!(offsets, [0, 0, 2, 2]);
    }

    #[test]
    fn csr_builder_rejects_non_monotonic_and_non_i64_offsets() {
        assert!(CsrBuilder::<u8>::with_capacity(usize::MAX, 0).is_err());
        assert!(CsrBuilder::<u8>::with_capacity(0, usize::MAX).is_err());

        let mut builder = CsrBuilder::<u8>::new();
        builder.push_end(3).expect("first end");
        assert!(builder.push_end(2).is_err());
        builder.push_row([2]).expect("reuse after invalid row end");
        let (values, offsets) = builder.finish();
        assert_eq!(values, [2]);
        assert_eq!(offsets, [0, 1]);

        let mut builder = CsrBuilder::<u8>::new();
        if usize::BITS > 63 {
            assert!(builder.push_end(i64::MAX as usize + 1).is_err());
            let (values, offsets) = builder.finish();
            assert!(values.is_empty());
            assert_eq!(offsets, [0]);
        }

        let mut builder = CsrBuilder::<u8>::new();
        builder.push_row([1]).expect("row");
        builder.clear();
        builder.push_row([2]).expect("reuse after clear");
        let (values, offsets) = builder.finish();
        assert_eq!(values, [2]);
        assert_eq!(offsets, [0, 1]);
    }

    #[test]
    fn csr_builder_treats_finite_hints_as_advisory() {
        let builder = CsrBuilder::<u8>::with_capacity_hint(usize::MAX, usize::MAX);
        let (values, offsets) = builder.finish();
        assert!(values.is_empty());
        assert_eq!(offsets, [0]);
    }

    #[test]
    fn string_collection_checks_native_pending_interrupt_before_exhaustion() {
        crate::test_support::initialize_python();
        Python::attach(|py| -> PyResult<()> {
            let _signal_guard = crate::test_support::PythonSignalTestGuard::new(py);
            let item = pyo3::types::PyString::new(py, "native-code");
            let total = 65;
            let iterator = crate::test_support::native_boundary_iterator(
                py,
                item.as_any(),
                total,
                Some(0),
                None,
            )?;
            let result = collect_py_strings(iterator.bind(py).as_any(), "code");
            let late_signal = py.check_signals().err();
            let error = match result {
                Err(error) => error,
                Ok(values) => panic!(
                    "native string collection completed after consuming {}; late signal={}",
                    values.len(),
                    late_signal.is_some()
                ),
            };

            assert!(error.is_instance_of::<pyo3::exceptions::PyKeyboardInterrupt>(py));
            assert!(
                late_signal.is_none(),
                "signal was observed after the target returned"
            );
            let state = iterator.bind(py).borrow();
            assert!(state.calls() < total, "finite provider was exhausted");
            assert!(
                state.remaining() > 0,
                "finite provider has no remaining item"
            );
            Ok(())
        })
        .expect("native string collection signal test");
    }

    #[test]
    fn string_collection_preserves_provider_exception_identity() {
        crate::test_support::initialize_python();
        Python::attach(|py| -> PyResult<()> {
            let _signal_guard = crate::test_support::PythonSignalTestGuard::new(py);
            let sentinel = pyo3::exceptions::PyRuntimeError::new_err("iterator sentinel")
                .value(py)
                .clone()
                .into_any()
                .unbind();
            let item = pyo3::types::PyString::new(py, "native-code");
            let iterator = crate::test_support::native_boundary_iterator(
                py,
                item.as_any(),
                3,
                None,
                Some((1, sentinel.clone_ref(py))),
            )?;
            let result = collect_py_strings(iterator.bind(py).as_any(), "code");
            let late_signal = py.check_signals().err();
            let error = result.expect_err("provider sentinel must be returned");

            assert!(late_signal.is_none());
            assert_eq!(error.value(py).as_ptr(), sentinel.as_ptr());
            let state = iterator.bind(py).borrow();
            assert_eq!(state.calls(), 2);
            assert_eq!(state.remaining(), 2);
            Ok(())
        })
        .expect("provider exception identity test");
    }

    #[test]
    fn string_collection_keeps_returned_wrong_type_as_type_error() {
        crate::test_support::initialize_python();
        Python::attach(|py| -> PyResult<()> {
            let _signal_guard = crate::test_support::PythonSignalTestGuard::new(py);
            let item = pyo3::types::PyInt::new(py, 7);
            let iterator =
                crate::test_support::native_boundary_iterator(py, item.as_any(), 1, None, None)?;
            let error = collect_py_strings(iterator.bind(py).as_any(), "code")
                .expect_err("integer item must be rejected");

            assert!(error.is_instance_of::<PyTypeError>(py));
            py.check_signals().unwrap();
            Ok(())
        })
        .expect("wrong item type test");
    }
}
