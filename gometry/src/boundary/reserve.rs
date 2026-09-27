//! Python collection admission and native capacity planning.
//!
//! Python `__len__` supplies an exact count for preallocation. Lengthless
//! iterables grow normally; receiver-sized inputs retain their cardinality
//! checks. Provider exceptions propagate and native allocator exhaustion aborts.

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyIterator, PySequence, PyString};

use crate::boundary::buffer_endian::{buffer_item_count, get_untyped_buffer};
use crate::error::{ResourceError, Result};

/// Initial reservation bound for advisory Rust iterator and serde size hints.
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
/// ``__len__`` belongs to the provider and must not be demoted to "no length".
pub(crate) fn optional_len(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
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

/// Append one value after rejecting the only recoverable failure: an
/// impossible element layout.
fn try_push_vec<T>(vec: &mut Vec<T>, value: T) -> Result<()> {
    if vec.len() == vec.capacity() {
        std::hint::cold_path();
        check_vec_capacity::<T>(vec.len(), 1)?;
    }
    vec.push(value);
    Ok(())
}

pub(crate) trait CsrOffset: Copy + Default + Ord {
    fn checked_end(end: usize) -> Result<Self>;
}

impl CsrOffset for i64 {
    fn checked_end(end: usize) -> Result<Self> {
        Self::try_from(end).map_err(|_| ResourceError::SizeOverflow.into())
    }
}

impl CsrOffset for i32 {
    fn checked_end(end: usize) -> Result<Self> {
        Self::try_from(end)
            .map_err(|_| crate::geometry::GeometryErrorKind::OffsetCapacityExceeded.into())
    }
}

/// One generic CSR owner for ragged output. Every row end is monotonic and
/// representable in its stored offset width before it is retained. Empty offsets
/// mean the implicit initial zero; the finished zero-row representation is `[0]`.
pub(crate) struct CsrBuilder<T, Offset = i64> {
    values: Vec<T>,
    offsets: Vec<Offset>,
}

impl<T, Offset: CsrOffset> CsrBuilder<T, Offset> {
    pub(crate) const fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Reserve exact, already-validated capacities for values and rows.
    pub(crate) fn with_capacity(value_capacity: usize, row_capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(value_capacity),
            offsets: Vec::with_capacity(row_capacity.wrapping_add(1)),
        }
    }

    pub(crate) fn reserve_rows(&mut self, additional: usize) {
        self.offsets
            .reserve(additional.wrapping_add(usize::from(self.offsets.is_empty())));
    }

    fn release(&mut self) {
        self.values = Vec::new();
        self.offsets = Vec::new();
    }

    pub(crate) fn push_row(&mut self, values: impl IntoIterator<Item = T>) -> Result<()> {
        self.values.extend(values);
        let end = self.values.len();
        self.push_end(end)
    }

    /// Append a checked row end after values have been emitted. This is also
    /// used by packed representations whose physical value count differs from
    /// the logical row count (for example, four vertices per triangle).
    pub(crate) fn push_end(&mut self, end: usize) -> Result<()> {
        let end = match Offset::checked_end(end) {
            Ok(end) => end,
            Err(error) => {
                self.release();
                return Err(error);
            },
        };
        let last = self.offsets.last().copied().unwrap_or_default();
        if end < last {
            self.release();
            return Err(crate::geometry::GeometryErrorKind::MalformedCsrOffsets.into());
        }
        if self.offsets.is_empty() {
            self.offsets.push(Offset::default());
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

    pub(crate) const fn values_mut(&mut self) -> &mut Vec<T> {
        &mut self.values
    }

    pub(crate) fn finish(self) -> (Vec<T>, Vec<Offset>) {
        let Self {
            values,
            mut offsets,
        } = self;
        if offsets.is_empty() {
            offsets.push(Offset::default());
        }
        (values, offsets)
    }
}

#[cfg(test)]
impl<T> CsrBuilder<T> {
    pub(crate) fn row_range(&self, row: usize) -> std::ops::Range<usize> {
        self.offsets[row] as usize..self.offsets[row + 1] as usize
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

/// Reserve from Python's exact length contract without limiting iteration.
///
/// Call after acquiring the iterator so an iterator-provider exception keeps
/// precedence over a `__len__` exception.
pub(crate) fn reserve_from_py_len<T>(
    values: &Bound<'_, PyAny>,
    output: &mut Vec<T>,
) -> PyResult<()> {
    if let Some(len) = optional_len(values)? {
        try_reserve_total(output, len)?;
    }
    Ok(())
}

/// Build an empty `Vec` from a validated external total.
pub(crate) fn try_vec_with_capacity<T>(capacity: usize) -> PyResult<Vec<T>> {
    let mut vec = Vec::new();
    try_reserve_total(&mut vec, capacity)?;
    Ok(vec)
}

/// Push one element while retaining a typed layout-overflow error.
pub(crate) fn try_push<T>(vec: &mut Vec<T>, item: T) -> PyResult<()> {
    try_push_vec(vec, item).map_err(PyErr::from)
}

/// Collect a bool mask aligned to the receiver's row count.
pub(crate) fn collect_bool_mask(
    mask: &Bound<'_, PyAny>,
    expected_len: usize,
) -> PyResult<Vec<bool>> {
    let iter = py_iter_or_error(mask, || {
        PyTypeError::new_err("mask must be a sequence or iterable of bool")
    })?;
    collect_py_iter_exact_from_iter(
        mask,
        iter,
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
}

/// Collect sequence elements once for classification and materialization.
pub(crate) fn collect_sequence_items(sequence: &Bound<'_, PySequence>) -> PyResult<Vec<Py<PyAny>>> {
    collect_py_iter(sequence.as_any(), |item| Ok(item.unbind()))
}

/// Collect a sequence of concrete byte payloads (pickle mixed rows). Each
/// element must be `bytes` / a buffer; never `Vec<u8>` from a generic sequence.
pub(crate) fn collect_bytes_rows(rows: &Bound<'_, PyAny>) -> PyResult<Vec<crate::OwnedBytes>> {
    let sequence = rows.cast::<PySequence>().map_err(|_| {
        PyTypeError::new_err("mixed geometry array pickle rows must be a sequence of bytes")
    })?;
    collect_py_iter(sequence.as_any(), |item| extract_bytes_payload(&item))
}

fn extract_bytes_payload(item: &Bound<'_, PyAny>) -> PyResult<crate::OwnedBytes> {
    if let Some(bytes) = crate::exact_backed_bytes(item) {
        return Ok(crate::OwnedBytes::Backed(bytes));
    }
    match copy_exported_byte_buffer(item) {
        Ok(bytes) => Ok(crate::OwnedBytes::Copied(bytes)),
        Err(ByteBufferCopyError::NotOneByte) => Err(PyTypeError::new_err(
            "mixed geometry array pickle rows must be bytes-like",
        )),
        Err(ByteBufferCopyError::PythonError(error)) => Err(error),
    }
}

/// Copy a one-byte Python buffer into exclusive Rust storage. This is the
/// owning counterpart to [`with_one_byte_buffer`].
/// The source producer must remain quiescent until the copy returns; owning
/// the result does not synchronize the source read.
pub(crate) fn copy_one_byte_buffer(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = value.extract::<&[u8]>() {
        return Ok(bytes.to_vec());
    }
    match copy_exported_byte_buffer(value) {
        Ok(bytes) => Ok(bytes),
        Err(ByteBufferCopyError::NotOneByte) => Err(PyTypeError::new_err(
            "expected a one-byte buffer (bytes, bytearray, memoryview, or array.array)",
        )),
        Err(ByteBufferCopyError::PythonError(error)) => Err(error),
    }
}

enum ByteBufferCopyError {
    NotOneByte,
    PythonError(PyErr),
}

fn copy_exported_byte_buffer(
    value: &Bound<'_, PyAny>,
) -> std::result::Result<Vec<u8>, ByteBufferCopyError> {
    let buffer = get_untyped_buffer(value)
        .map_err(ByteBufferCopyError::PythonError)?
        .ok_or(ByteBufferCopyError::NotOneByte)?;
    if let Ok(buffer) = buffer.as_typed::<u8>() {
        let item_count = buffer_item_count(buffer).map_err(ByteBufferCopyError::PythonError)?;
        let mut owned = vec![0; item_count];
        if item_count != 0 {
            buffer
                .copy_to_slice(value.py(), &mut owned)
                .map_err(ByteBufferCopyError::PythonError)?;
        }
        return Ok(owned);
    }
    if let Ok(buffer) = buffer.as_typed::<i8>() {
        let item_count = buffer_item_count(buffer).map_err(ByteBufferCopyError::PythonError)?;
        let mut signed = vec![0; item_count];
        if item_count != 0 {
            buffer
                .copy_to_slice(value.py(), &mut signed)
                .map_err(ByteBufferCopyError::PythonError)?;
        }
        let (ptr, len, capacity) = signed.into_raw_parts();
        // SAFETY: i8 and u8 have the same layout and every byte is initialized.
        return Ok(unsafe { Vec::from_raw_parts(ptr.cast(), len, capacity) });
    }
    Err(ByteBufferCopyError::NotOneByte)
}

/// Collect Python strings while preserving provider and type errors.
pub(crate) fn collect_py_strings(values: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<String>> {
    let iter = py_iter_or_error(values, || {
        PyTypeError::new_err(format!("{what} must be an iterable of strings"))
    })?;
    let mut out = Vec::new();
    reserve_from_py_len(values, &mut out)?;
    for (index, item) in iter.enumerate() {
        let item = item?;
        check_python_signals(values.py(), index)?;
        let text = item
            .cast::<PyString>()
            .map_err(|_| PyTypeError::new_err(format!("{what} elements have the wrong type")))?;
        out.push(text.to_str()?.to_owned());
    }
    Ok(out)
}

/// True when ``value`` exports a one-byte buffer (signed or unsigned item
/// format): ``bytes``/``bytearray``/``memoryview``/``array.array('B'|'b')``.
pub(crate) fn is_one_byte_buffer(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.extract::<&[u8]>().is_ok() {
        return Ok(true);
    }
    let Some(buffer) = get_untyped_buffer(value)? else {
        return Ok(false);
    };
    Ok(buffer.as_typed::<u8>().is_ok() || buffer.as_typed::<i8>().is_ok())
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
    let owned = copy_one_byte_buffer(value)?;
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

/// Collect Python items with optional mapping and exact-length preallocation.
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
    iter: Bound<'py, PyIterator>,
    mut map: impl FnMut(Bound<'py, PyAny>) -> PyResult<T>,
) -> PyResult<Vec<T>> {
    let mut out = Vec::new();
    reserve_from_py_len(values, &mut out)?;
    for (index, item) in iter.enumerate() {
        let item = item?;
        check_python_signals(values.py(), index)?;
        out.push(map(item)?);
    }
    Ok(out)
}

/// Visit an already-acquired Python iterator with the shared signal cadence.
pub(crate) fn visit_py_iter_from_iter<'py>(
    values: &Bound<'py, PyAny>,
    iter: Bound<'py, PyIterator>,
    mut visit: impl FnMut(Bound<'py, PyAny>) -> PyResult<()>,
) -> PyResult<()> {
    for (index, item) in iter.enumerate() {
        let item = item?;
        check_python_signals(values.py(), index)?;
        visit(item)?;
    }
    Ok(())
}

/// Collect **exactly** `expected` items from a Python iterable.
///
/// Stops after `expected + 1` elements so an unbounded iterator (e.g.
/// `itertools.repeat`) cannot grow forever when the length is known from the
/// receiver. The receiver's count sizes the output reservation.
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
        out.push(map(item?)?);
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
        out.push(map(item?)?);
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
    fn try_push_has_no_element_count_ceiling() {
        // Spot-check that ordinary growth has no advisory-hint ceiling.
        let mut v: Vec<u8> = Vec::new();
        for i in 0..10_000 {
            try_push(&mut v, (i & 0xFF) as u8).expect("no artificial count ceiling");
        }
        assert_eq!(v.len(), 10_000);
    }

    #[test]
    fn csr_builder_preserves_empty_rows_and_checked_offsets() {
        let mut builder = CsrBuilder::<u8>::with_capacity(4, 3);
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

    #[test]
    fn string_collection_preserves_unicode_conversion_error() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let values = py.eval(c"['valid', '\\ud800']", None, None).unwrap();
            let error = collect_py_strings(&values, "code")
                .expect_err("unpaired surrogate has no UTF-8 representation");
            assert!(error.is_instance_of::<pyo3::exceptions::PyUnicodeEncodeError>(py));
        });
    }
}
