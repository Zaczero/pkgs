//! Checked reservation for untrusted external counts and length hints.
//!
//! Public Arrow lengths, Python `__len__` values, and multi-chunk coordinate
//! totals are attacker-controlled. Infallibly calling `Vec::with_capacity` on
//! them panics with capacity overflow; this module is the single fallible
//! reservation keystone used at every such site.
//!
//! ## Hints vs validated totals
//!
//! - **Hints** (`try_reserve_hint`): Python `__len__` and similar presizing
//!   guesses. Oversized hints are **clamped** (never a rejection that changes
//!   validity); growth remains fallible via `try_reserve`.
//! - **Validated totals** (`try_reserve_total` / `try_vec_with_capacity`):
//!   checked sums of Arrow lengths and other exact, already-validated counts.
//!   These use **uncapped** fallible reservation so multi-chunk import cannot
//!   reject a total that single-chunk would accept. The allocator may still
//!   fail cleanly with `MemoryError`.
//!
//! ## No artificial element-count caps
//!
//! Valid large finite inputs (lists, generators of the same length) must share
//! one outcome: success or clean proportional `MemoryError` from the allocator.
//! Never reject with "too large to materialize" or a bare-stream ceiling; that
//! invents a list-vs-generator asymmetry. The collector's retained output
//! grows fallibly; a caller's per-item mapping must also use fallible
//! allocation when it owns separate storage on an unbounded path (see
//! [`collect_py_iter`]).

use std::collections::BinaryHeap;

use pyo3::exceptions::{PyMemoryError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyIterator, PySequence};

use crate::error::{ResourceError, Result};

/// Soft upper bound for a single **initial** reservation from an external
/// length *hint* (`__len__`, etc.). Large enough to avoid thrash on typical
/// batch sizes; small enough that a lying `sys.maxsize` / `i64::MAX` hint
/// cannot force a multi-gigabyte empty `Vec` before the first item is
/// yielded. Growth after the first item stays fallible via `try_reserve` /
/// `try_reserve_next` on actual observed elements — never promote a hint to
/// an exact allocation or iteration count (AGENTS carrier 3).
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

/// Reserve output storage without constructing an error while the old buffer
/// is still retained. The caller owns the Python conversion of the compact
/// resource error.
pub(crate) fn try_reserve_vec<T>(vec: &mut Vec<T>, additional: usize) -> Result<()> {
    if additional == 0 {
        return Ok(());
    }
    vec.try_reserve(additional).map_err(|_| {
        *vec = Vec::new();
        ResourceError::Allocation.into()
    })
}

/// Fallibly append one value to Rust-owned output storage. The value and the
/// retained vector are released before the allocation error is returned.
pub(crate) fn try_push_vec<T>(vec: &mut Vec<T>, value: T) -> Result<()> {
    if vec.len() == vec.capacity() {
        let additional = vec.capacity().max(8);
        if vec.try_reserve(additional).is_err() {
            drop(value);
            *vec = Vec::new();
            return Err(ResourceError::Allocation.into());
        }
    }
    vec.push(value);
    Ok(())
}

/// Reserve two parallel output columns as one fallible operation. If the
/// second reservation fails, release the first column before the caller maps
/// the resource error to a Python exception.
pub(crate) fn try_reserve_pair<T, U>(
    left: &mut Vec<T>,
    right: &mut Vec<U>,
    additional: usize,
) -> Result<()> {
    if let Err(error) = try_reserve_vec(left, additional) {
        *right = Vec::new();
        return Err(error);
    }
    if let Err(error) = try_reserve_vec(right, additional) {
        *left = Vec::new();
        return Err(error);
    }
    Ok(())
}

/// Append one item to each of two parallel output columns. A failure in the
/// second append releases both columns before the error is converted.
pub(crate) fn try_push_pair<T, U>(
    left: &mut Vec<T>,
    right: &mut Vec<U>,
    left_value: T,
    right_value: U,
) -> Result<()> {
    if let Err(error) = try_push_vec(left, left_value) {
        drop(right_value);
        *right = Vec::new();
        return Err(error);
    }
    if let Err(error) = try_push_vec(right, right_value) {
        *left = Vec::new();
        return Err(error);
    }
    Ok(())
}

/// Fallibly append one ordered work item to a heap used as a traversal
/// frontier. The heap is released before returning an allocation error.
pub(crate) fn try_push_heap<T: Ord>(heap: &mut BinaryHeap<T>, value: T) -> Result<()> {
    if heap.len() == heap.capacity() {
        let additional = heap.capacity().max(8);
        if heap.try_reserve(additional).is_err() {
            drop(value);
            *heap = BinaryHeap::new();
            return Err(ResourceError::Allocation.into());
        }
    }
    heap.push(value);
    Ok(())
}

/// Fallibly reserve a known heap capacity, releasing the heap if the
/// allocation is refused.
pub(crate) fn try_reserve_heap<T>(heap: &mut BinaryHeap<T>, additional: usize) -> Result<()> {
    if additional == 0 {
        return Ok(());
    }
    heap.try_reserve(additional).map_err(|_| {
        *heap = BinaryHeap::new();
        ResourceError::Allocation.into()
    })
}

/// One generic CSR owner for ragged output. Values grow fallibly and every
/// row end is checked, monotonic, and representable as `i64` before it is
/// retained. The zero-row representation is the valid offsets column `[0]`.
pub(crate) struct CsrBuilder<T> {
    values: Vec<T>,
    offsets: Vec<i64>,
}

impl<T> CsrBuilder<T> {
    pub(crate) fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: vec![0],
        }
    }

    /// Reserve exact, already-validated capacities for values and rows.
    pub(crate) fn with_capacity(value_capacity: usize, row_capacity: usize) -> Result<Self> {
        let mut builder = Self::new();
        try_reserve_vec(&mut builder.values, value_capacity)?;
        let rows = row_capacity
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        if let Err(error) = try_reserve_vec(&mut builder.offsets, rows.saturating_sub(1)) {
            builder.release();
            return Err(error);
        }
        Ok(builder)
    }

    /// Reserve only a bounded initial chunk from advisory external hints.
    /// Hints never affect validity; observed values still grow through the
    /// same fallible path.
    #[cfg(test)]
    pub(crate) fn with_capacity_hint(value_hint: usize, row_hint: usize) -> Result<Self> {
        Self::with_capacity(
            value_hint.min(MAX_RESERVE_HINT),
            row_hint.min(MAX_RESERVE_HINT),
        )
    }

    fn release(&mut self) {
        self.values = Vec::new();
        self.offsets = vec![0];
    }

    pub(crate) fn push(&mut self, value: T) -> Result<()> {
        if let Err(error) = try_push_vec(&mut self.values, value) {
            self.offsets = Vec::new();
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn push_row(&mut self, values: impl IntoIterator<Item = T>) -> Result<()> {
        for value in values {
            self.push(value)?;
        }
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
        let last = *self
            .offsets
            .last()
            .expect("CSR builder always starts with a zero offset");
        if end < last {
            self.release();
            return Err(crate::geometry::GeometryErrorKind::MalformedCsrOffsets.into());
        }
        if let Err(error) = try_push_vec(&mut self.offsets, end) {
            self.release();
            return Err(error);
        }
        Ok(())
    }

    pub(crate) const fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn clear(&mut self) {
        self.release();
    }

    #[cfg(test)]
    pub(crate) fn values(&self) -> &[T] {
        &self.values
    }

    pub(crate) const fn values_mut(&mut self) -> &mut Vec<T> {
        &mut self.values
    }

    #[cfg(test)]
    pub(crate) fn offsets(&self) -> &[i64] {
        &self.offsets
    }

    #[cfg(test)]
    pub(crate) fn row_range(&self, row: usize) -> std::ops::Range<usize> {
        self.offsets[row] as usize..self.offsets[row + 1] as usize
    }

    pub(crate) fn finish(self) -> (Vec<T>, Vec<i64>) {
        (self.values, self.offsets)
    }
}

/// Fallible collection for a Python-facing result iterator. Unlike
/// `Iterator::collect`, outer growth is routed through the shared reserve
/// policy and retained output is dropped by `try_push` on allocation failure.
pub(crate) fn try_collect_results<T>(
    values: impl IntoIterator<Item = PyResult<T>>,
) -> PyResult<Vec<T>> {
    let mut out = Vec::new();
    for value in values {
        let value = match value {
            Ok(value) => value,
            Err(error) => {
                drop(out);
                return Err(error);
            },
        };
        try_push(&mut out, value)?;
    }
    Ok(out)
}

/// Fallibly reserve capacity for `additional` more elements, treating the
/// count as an untrusted **hint**. Oversized hints are clamped to
/// [`MAX_RESERVE_HINT`] (never rejected); `try_reserve` turns overflow / OOM
/// into a clean Python error instead of a panic. This only sizes the initial
/// chunk — honest large streams grow fallibly as items arrive.
pub(crate) fn try_reserve_hint<T>(vec: &mut Vec<T>, additional: usize) -> PyResult<()> {
    let additional = additional.min(MAX_RESERVE_HINT);
    if additional == 0 {
        return Ok(());
    }
    if vec.try_reserve(additional).is_err() {
        *vec = Vec::new();
        return Err(crate::py::errors::native_memory_error());
    }
    Ok(())
}

/// Fallibly reserve capacity for an exact, **validated** total (checked Arrow
/// length sums, known array lengths, etc.). No soft cap — multi-chunk and
/// single-chunk paths share the same validity; only the allocator can fail.
pub(crate) fn try_reserve_total<T>(vec: &mut Vec<T>, additional: usize) -> PyResult<()> {
    if additional == 0 {
        return Ok(());
    }
    if vec.try_reserve(additional).is_err() {
        *vec = Vec::new();
        return Err(crate::py::errors::native_memory_error());
    }
    Ok(())
}

/// Build an empty `Vec` with a fallible capacity reservation from a validated
/// total (checked sum of external lengths, etc.). Uncapped — see
/// [`try_reserve_total`].
pub(crate) fn try_vec_with_capacity<T>(capacity: usize) -> PyResult<Vec<T>> {
    let mut vec = Vec::new();
    try_reserve_total(&mut vec, capacity)?;
    Ok(vec)
}

/// Build an empty `Vec` from an untrusted length hint (clamped).
pub(crate) fn try_vec_with_capacity_hint<T>(capacity: usize) -> PyResult<Vec<T>> {
    let mut vec = Vec::new();
    try_reserve_hint(&mut vec, capacity)?;
    Ok(vec)
}

/// Probe whether `vec` needs a growth reservation; fallible, no Python error
/// yet.
///
/// Returns `Err(())` on allocator failure. Callers that already hold retained
/// untrusted output **must drop that output before** converting to
/// a Python exception — materializing a Rust `PyErr` while the heap is
/// exhausted aborts with `memory allocation of N bytes failed` instead of a
/// catchable error.
fn try_reserve_next_raw<T>(vec: &mut Vec<T>) -> Result<(), ()> {
    if vec.len() == vec.capacity() {
        // Double (or start at 8); fallible so capacity overflow is a clean error.
        let additional = vec.capacity().max(8);
        vec.try_reserve(additional).map_err(|_| ())?;
    }
    Ok(())
}

/// Fallibly push one element, growing capacity without trusting a bulk hint.
///
/// On OOM the **item is dropped** with the vector contents before the native
/// exception is raised, so Python error adoption has address-space headroom
/// under `RLIMIT_AS`.
pub(crate) fn try_push<T>(vec: &mut Vec<T>, item: T) -> PyResult<()> {
    if try_reserve_next_raw(vec).is_err() {
        drop(item);
        // Drop the allocation before raising the native error; assignment does
        // not request replacement storage when the heap is exhausted.
        *vec = Vec::new();
        return Err(crate::py::errors::native_memory_error());
    }
    vec.push(item);
    Ok(())
}

/// Fallibly copy a UTF-8 slice into an owned [`String`].
///
/// `Err(())` means the allocator refused the reservation — **do not** build a
/// `PyErr` until any large retained buffers that exhausted memory have been
/// dropped (see [`collect_py_iter`]). Use the native memory-error owner after
/// free.
///
/// On unbounded ingest paths, `String::from` / `extract::<String>` allocate
/// infallibly and abort under OOM instead of surfacing `MemoryError`.
pub(crate) fn try_string_from_str(s: &str) -> Result<String, ()> {
    let mut buf = Vec::<u8>::new();
    buf.try_reserve_exact(s.len()).map_err(|_| ())?;
    buf.extend_from_slice(s.as_bytes());
    // SAFETY: `s` is valid UTF-8; we only copied its bytes into reserved space.
    Ok(unsafe { String::from_utf8_unchecked(buf) })
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

/// Collect sequence elements once (classify + materialize in one pass) with
/// fallible reservation. Never uses generic `Vec<T>: FromPyObject`.
pub(crate) fn collect_sequence_items(sequence: &Bound<'_, PySequence>) -> PyResult<Vec<Py<PyAny>>> {
    // Prefer the iterator protocol so a lying `__len__` cannot force a huge
    // empty allocation; fallible per-item growth handles real large sequences.
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
pub(crate) fn collect_bytes_rows(rows: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
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

fn extract_bytes_payload(item: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    // Shared one-byte buffer path (signed + unsigned); never generic
    // sequence-of-int extract (that path allocates from a lying `__len__`).
    copy_one_byte_buffer(item).map_err(|error| {
        if error.is_instance_of::<PyMemoryError>(item.py()) {
            error
        } else {
            PyTypeError::new_err("mixed geometry array pickle rows must be bytes-like")
        }
    })
}

/// Copy a one-byte Python buffer into exclusive Rust storage with fallible
/// allocation. This is the owning counterpart to [`with_one_byte_buffer`].
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

/// Collect Python strings without making an infallible owned copy per item.
/// Allocation failure drops already-retained strings before the Python error
/// object is constructed.
pub(crate) fn collect_py_strings(values: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<String>> {
    let mut out = Vec::new();
    if let Some(hint) = advisory_len(values)? {
        try_reserve_hint(&mut out, hint)?;
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
        let Ok(owned) = try_string_from_str(text) else {
            drop(out);
            return Err(crate::py::errors::native_memory_error());
        };
        if try_reserve_next_raw(&mut out).is_err() {
            drop(owned);
            drop(out);
            return Err(crate::py::errors::native_memory_error());
        }
        out.push(owned);
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
/// before parse — a `PyBuffer` pins the allocation but does not stop another
/// thread mutating contents, and forming a plain `&[u8]` over that memory is
/// aliasing UB.
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
/// exporter's memory — so concurrent free-threaded mutators of the original
/// `bytearray` / writable `memoryview` cannot alias through this borrow.
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
    // Copy into exclusively owned Rust storage with a fallible reservation.
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
    // fallibly allocated buffer instead of retaining two copies.
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

/// Collect `usize` handles for pickle reconstructors (spatial index live rows).
pub(crate) fn collect_usize_sequence(value: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<usize>> {
    collect_py_iter(value, |item| try_usize_item(&item, what))
}

/// Collect `i64` CSR offsets / int groups for pickle reconstructors.
pub(crate) fn collect_i64_sequence(value: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<i64>> {
    collect_py_iter(value, |item| try_i64_item(&item, what))
}

fn try_u64_item(value: &Bound<'_, PyAny>, what: &str) -> PyResult<u64> {
    match crate::py_u64_required(what, value) {
        Ok(value) => Ok(value),
        Err(error)
            if error.is_instance_of::<PyTypeError>(value.py())
                && value
                    .getattr_opt(pyo3::intern!(value.py(), "__index__"))?
                    .is_none() =>
        {
            Err(PyTypeError::new_err(format!(
                "{what} elements have the wrong type"
            )))
        },
        Err(error) => Err(error),
    }
}

fn try_usize_item(value: &Bound<'_, PyAny>, what: &str) -> PyResult<usize> {
    match crate::py_usize_required(what, value) {
        Ok(value) => Ok(value),
        Err(error)
            if error.is_instance_of::<PyTypeError>(value.py())
                && value
                    .getattr_opt(pyo3::intern!(value.py(), "__index__"))?
                    .is_none() =>
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
            if error.is_instance_of::<PyTypeError>(value.py())
                && value
                    .getattr_opt(pyo3::intern!(value.py(), "__index__"))?
                    .is_none() =>
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
/// `__len__` is a clamped **hint** only; every element is grown via
/// fallible reservation before the map runs. Callers that need a mapped type
/// pass a closure; the identity map (`|item| Ok(item)`) materializes the raw
/// `Bound` items.
///
/// Prefer this over `iter.collect::<PyResult<Vec<_>>>()` or bare `Vec::push`
/// loops — those grow infallibly until Rust aborts on capacity overflow / OOM.
/// There is no bare-stream element ceiling: a finite generator of N elements
/// succeeds the same way as a list of N.
///
/// ## Full no-abort invariant (outer growth **and** the map)
///
/// Reservation-before-map alone is **not** the complete boundary. It only
/// makes growth of the outer `Vec<T>` fallible. If `map` itself performs an
/// infallible Rust heap allocation (e.g. `extract::<String>()`, `String::from`,
/// an uncapped `Vec` push, `Arc::from` of a slice), that allocation can still
/// abort under `RLIMIT_AS` with `memory allocation of N bytes failed` while
/// the outer `try_reserve` still has room for another `T` slot. On unbounded
/// public ingest paths the map must therefore either:
/// - perform no per-item Rust heap allocation beyond what this collector
///   already reserved (identity / `Copy` / Arc-clone of an existing handle), or
/// - route every new owned allocation through a fallible path
///   ([`try_string_from_str`], nested `collect_py_iter` / `try_push`, etc.).
///
/// ## Drop retained output before raising
///
/// Under a tight address-space cap the retained `Vec` may be what exhausted
/// the heap. Constructing a Rust `PyErr` while that `Vec` still holds every
/// item aborts instead of raising. This collector drops `out` before any OOM
/// error is built; maps that build a Python error on
/// their own OOM path must use [`try_string_from_str`]'s `Err(())` form (or
/// equivalent) so the collector can free first — see
/// [`crate::py::crs::functions_config`] for the search-path loop.
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
        try_reserve_hint(&mut out, hint)?;
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
        // Reserve a slot for `T` before mapping so outer growth is fallible.
        // Free retained items before raising the native error (see invariant).
        if try_reserve_next_raw(&mut out).is_err() {
            drop(out);
            return Err(crate::py::errors::native_memory_error());
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
/// receiver. Uses fallible exact reservation for the validated count.
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
        // try_push frees `out` on OOM before boxing PyErr.
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

    use pyo3::exceptions::PyMemoryError;

    use super::*;
    use crate::test_support::fail_allocations_after;

    #[test]
    fn reserve_hint_clamps_instead_of_rejecting() {
        let mut v: Vec<u8> = Vec::new();
        // Formerly raised ValueError for additional > MAX_RESERVE_HINT.
        try_reserve_hint(&mut v, MAX_RESERVE_HINT + 1).expect("hint must clamp, not reject");
        assert!(v.capacity() <= MAX_RESERVE_HINT);
        assert!(v.capacity() > 0);
    }

    #[test]
    fn reserve_total_accepts_above_hint_cap() {
        let mut v: Vec<u8> = Vec::new();
        // Validated totals must not share the hint cap as a rejection.
        // Use u8 so the allocation stays ~64 MiB + 1, not f64-scale.
        match try_reserve_total(&mut v, MAX_RESERVE_HINT + 1) {
            Ok(()) => assert!(v.capacity() > MAX_RESERVE_HINT),
            Err(err) => {
                // OOM is acceptable; a "maximum reserve" ValueError is not.
                let msg = err.to_string();
                assert!(
                    !msg.contains("maximum reserve")
                        && !msg.contains("exceeds maximum")
                        && !msg.contains("too large to materialize"),
                    "validated total must not hit the hint cap: {msg}"
                );
            },
        }
    }

    #[test]
    fn try_push_grows_fallibly_without_bulk_capacity_panic() {
        // Element-by-element growth is the R01/R02 keystone path: a lying
        // bulk hint never reaches `Vec::with_capacity`.
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
    fn try_push_releases_retained_output_before_native_memory_error() {
        Python::initialize();
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

            let dropped = Rc::new(RefCell::new(Vec::with_capacity(3)));
            let probe = |id| DropProbe {
                id,
                dropped: Rc::clone(&dropped),
            };
            let mut values = Vec::with_capacity(2);
            values.push(probe(1));
            values.push(probe(2));
            let result = fail_allocations_after(0, || try_push(&mut values, probe(3)));
            let error = result.expect_err("the persistent allocator must reject growth");
            assert!(error.is_instance_of::<PyMemoryError>(py));
            let mut ids = dropped.borrow().clone();
            ids.sort_unstable();
            assert_eq!(ids, [1, 2, 3]);
            drop(error);
        });
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
        // N3: try_push must not hard-reject past MAX_RESERVE_HINT; growth is
        // fallible-only. Spot-check past a former backstop size is impractical
        // (64 Mi), so assert the capacity path has no len-gate by growing a
        // small buffer far past the doubling start without a count reject.
        let mut v: Vec<u8> = Vec::new();
        for i in 0..10_000 {
            try_push(&mut v, (i & 0xFF) as u8).expect("no artificial count ceiling");
        }
        assert_eq!(v.len(), 10_000);
    }

    #[test]
    fn try_vec_with_capacity_hint_clamps_absurd_counts() {
        let v: Vec<u8> = try_vec_with_capacity_hint(usize::MAX / 8).expect("hint clamps");
        assert!(v.capacity() <= MAX_RESERVE_HINT);
    }

    #[test]
    fn csr_builder_preserves_empty_rows_and_checked_offsets() {
        let mut builder = CsrBuilder::<u8>::with_capacity_hint(4, 3).expect("small hint");
        builder.push_row(std::iter::empty()).expect("empty row");
        builder.push_row([1, 2]).expect("values");
        builder
            .push_row(std::iter::empty())
            .expect("second empty row");
        assert_eq!(builder.values(), &[1, 2]);
        assert_eq!(builder.offsets(), &[0, 0, 2, 2]);
        assert_eq!(builder.row_range(1), 0..2);
    }

    #[test]
    fn csr_builder_rejects_non_monotonic_and_non_i64_offsets() {
        let mut builder = CsrBuilder::<u8>::new();
        builder.push_end(3).expect("first end");
        assert!(builder.push_end(2).is_err());
        assert!(builder.values().is_empty());
        assert_eq!(builder.offsets(), &[0]);

        let mut builder = CsrBuilder::<u8>::new();
        if usize::BITS > 63 {
            assert!(builder.push_end(i64::MAX as usize + 1).is_err());
            assert_eq!(builder.offsets(), &[0]);
        } else {
            assert!(CsrBuilder::<u8>::with_capacity(0, usize::MAX).is_err());
        }

        let mut builder = CsrBuilder::<u8>::new();
        builder.push_row([1]).expect("row");
        builder.clear();
        builder.push_row([2]).expect("reuse after clear");
        assert_eq!(builder.values(), &[2]);
        assert_eq!(builder.offsets(), &[0, 1]);
    }

    #[test]
    fn csr_builder_treats_finite_hints_as_advisory() {
        let builder = CsrBuilder::<u8>::with_capacity_hint(usize::MAX, usize::MAX)
            .expect("lying finite hint is clamped");
        assert!(builder.values().is_empty());
        assert_eq!(builder.offsets(), &[0]);
    }

    #[test]
    fn failed_growth_releases_retained_storage_before_error() {
        let mut values = vec![1_u8, 2, 3];
        assert!(try_reserve_vec(&mut values, usize::MAX).is_err());
        assert!(values.is_empty());
        assert_eq!(values.capacity(), 0);
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
            assert!(py.check_signals().is_ok());
            Ok(())
        })
        .expect("wrong item type test");
    }
}
