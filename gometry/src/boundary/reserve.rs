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
//! grows fallibly; a caller's per-item mapping may own separate storage.

use pyo3::exceptions::{PyMemoryError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PySequence;

/// Soft upper bound for a single reservation from an external length *hint*.
/// Large enough for legitimate multi-million-row columns; small enough that a
/// `sys.maxsize` / `i64::MAX` hint cannot force a multi-gigabyte empty `Vec`.
/// Applied only as a clamp on hints — never as a product-wide validity cap.
pub(crate) const MAX_RESERVE_HINT: usize = 1 << 26; // 64 Mi elements

/// Fallibly reserve capacity for `additional` more elements, treating the
/// count as an untrusted **hint**. Oversized hints are clamped to
/// [`MAX_RESERVE_HINT`] (never rejected); `try_reserve` turns overflow / OOM
/// into a clean Python error instead of a panic.
pub(crate) fn try_reserve_hint<T>(vec: &mut Vec<T>, additional: usize) -> PyResult<()> {
    let additional = additional.min(MAX_RESERVE_HINT);
    if additional == 0 {
        return Ok(());
    }
    vec.try_reserve(additional).map_err(|_| {
        PyMemoryError::new_err(format!(
            "failed to reserve capacity for {additional} elements from untrusted length"
        ))
    })
}

/// Fallibly reserve capacity for an exact, **validated** total (checked Arrow
/// length sums, known array lengths, etc.). No soft cap — multi-chunk and
/// single-chunk paths share the same validity; only the allocator can fail.
pub(crate) fn try_reserve_total<T>(vec: &mut Vec<T>, additional: usize) -> PyResult<()> {
    if additional == 0 {
        return Ok(());
    }
    vec.try_reserve(additional).map_err(|_| {
        PyMemoryError::new_err(format!(
            "failed to reserve capacity for {additional} elements"
        ))
    })
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

/// Fallibly push one element, growing capacity without trusting a bulk hint.
/// Used when collecting untrusted Python sequences that must never call
/// `Vec::with_capacity` on a lying `__len__`.
///
/// Growth is pure fallible reservation — no element-count ceiling. This
/// protects the retained vector's allocation boundary; callers that map input
/// items own any separate per-item allocation boundary.
fn try_reserve_next<T>(vec: &mut Vec<T>) -> PyResult<()> {
    if vec.len() == vec.capacity() {
        // Double (or start at 8); fallible so capacity overflow is a clean error.
        let additional = vec.capacity().max(8);
        vec.try_reserve(additional).map_err(|_| {
            PyMemoryError::new_err("failed to grow sequence buffer from untrusted input")
        })?;
    }
    Ok(())
}

pub(crate) fn try_push<T>(vec: &mut Vec<T>, item: T) -> PyResult<()> {
    try_reserve_next(vec)?;
    vec.push(item);
    Ok(())
}

/// Checked sum of untrusted lengths. Overflow becomes a clean value error.
pub(crate) fn checked_length_sum(
    lengths: impl IntoIterator<Item = PyResult<usize>>,
) -> PyResult<usize> {
    let mut total = 0_usize;
    for length in lengths {
        let length = length?;
        total = total
            .checked_add(length)
            .ok_or_else(|| PyValueError::new_err("total length from untrusted inputs overflows"))?;
    }
    Ok(total)
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
    // Fast path: exact list/tuple of the right length via per-item extract.
    if let Ok(sequence) = mask.cast::<PySequence>() {
        // Capture length once for the mismatch check only — never as
        // `Vec::with_capacity` input via FromPyObject.
        let reported = sequence.len()?;
        if reported != expected_len {
            return Err(PyValueError::new_err(format!(
                "mask length {reported} does not match array length {expected_len}"
            )));
        }
        let mut out = try_vec_with_capacity(expected_len)?;
        for index in 0..expected_len {
            let item = sequence.get_item(index)?;
            let bit: bool = item
                .extract()
                .map_err(|_| PyTypeError::new_err("mask elements must be bool"))?;
            out.push(bit);
        }
        return Ok(out);
    }
    // Iterable fallback (no __len__): collect with fallible growth, then check.
    if let Ok(iter) = mask.try_iter() {
        let mut out = Vec::new();
        for item in iter {
            let item = item?;
            let bit: bool = item
                .extract()
                .map_err(|_| PyTypeError::new_err("mask elements must be bool"))?;
            try_push(&mut out, bit)?;
            if out.len() > expected_len {
                return Err(PyValueError::new_err(format!(
                    "mask length {} does not match array length {expected_len}",
                    out.len()
                )));
            }
        }
        if out.len() != expected_len {
            return Err(PyValueError::new_err(format!(
                "mask length {} does not match array length {expected_len}",
                out.len()
            )));
        }
        return Ok(out);
    }
    Err(PyTypeError::new_err(
        "mask must be a sequence or iterable of bool",
    ))
}

/// Collect sequence elements once (classify + materialize in one pass) with
/// fallible reservation. Never uses generic `Vec<T>: FromPyObject`.
pub(crate) fn collect_sequence_items(sequence: &Bound<'_, PySequence>) -> PyResult<Vec<Py<PyAny>>> {
    // Prefer the iterator protocol so a lying `__len__` cannot force a huge
    // empty allocation; fallible per-item growth handles real large sequences.
    let any = sequence.as_any();
    if let Ok(iter) = any.try_iter() {
        let mut out = Vec::new();
        for item in iter {
            try_push(&mut out, item?.unbind())?;
        }
        return Ok(out);
    }
    // Fallback: capture len once as a clamped *hint*, then get_item. Growth is
    // fallible; no hard "too large to materialize" reject of valid lengths.
    let reported = sequence.len()?;
    let mut out = try_vec_with_capacity_hint(reported)?;
    for index in 0..reported {
        try_push(&mut out, sequence.get_item(index)?.unbind())?;
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
    if let Ok(iter) = any.try_iter() {
        for item in iter {
            let item = item?;
            let bytes = extract_bytes_payload(&item)?;
            try_push(&mut out, bytes)?;
        }
        return Ok(out);
    }
    let reported = sequence.len()?;
    out = try_vec_with_capacity_hint(reported)?;
    for index in 0..reported {
        let item = sequence.get_item(index)?;
        try_push(&mut out, extract_bytes_payload(&item)?)?;
    }
    Ok(out)
}

fn extract_bytes_payload(item: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    // Shared one-byte buffer path (signed + unsigned); never generic
    // sequence-of-int extract (that path allocates from a lying `__len__`).
    with_one_byte_buffer(item, |bytes| Ok(bytes.to_vec()))
        .map_err(|_| PyTypeError::new_err("mixed geometry array pickle rows must be bytes-like"))
}

/// True when ``value`` exports a one-byte buffer (signed or unsigned item
/// format): ``bytes``/``bytearray``/``memoryview``/``array.array('B'|'b')``.
pub(crate) fn is_one_byte_buffer(value: &Bound<'_, PyAny>) -> bool {
    value.extract::<&[u8]>().is_ok()
        || pyo3::buffer::PyBuffer::<u8>::get(value).is_ok()
        || pyo3::buffer::PyBuffer::<i8>::get(value).is_ok()
}

/// Borrow the raw bytes of any one-byte buffer and hand them to ``f``.
///
/// Accepts ``bytes``/``bytearray``/``memoryview`` and other buffer-protocol
/// exporters with itemsize 1 — both unsigned (``'B'``) and signed (``'b'``).
/// The buffer is held for the duration of ``f``.
pub(crate) fn with_one_byte_buffer<R>(
    value: &Bound<'_, PyAny>,
    f: impl FnOnce(&[u8]) -> PyResult<R>,
) -> PyResult<R> {
    if let Ok(bytes) = value.extract::<&[u8]>() {
        return f(bytes);
    }
    if let Ok(buffer) = pyo3::buffer::PyBuffer::<u8>::get(value) {
        return with_typed_one_byte_buffer(&buffer, value.py(), f);
    }
    if let Ok(buffer) = pyo3::buffer::PyBuffer::<i8>::get(value) {
        return with_typed_one_byte_buffer(&buffer, value.py(), f);
    }
    Err(PyTypeError::new_err(
        "expected a one-byte buffer (bytes, bytearray, memoryview, or array.array)",
    ))
}

fn with_typed_one_byte_buffer<T: pyo3::buffer::Element + Copy, R>(
    buffer: &pyo3::buffer::PyBuffer<T>,
    py: Python<'_>,
    f: impl FnOnce(&[u8]) -> PyResult<R>,
) -> PyResult<R> {
    if buffer.item_size() != 1 {
        return Err(PyTypeError::new_err(
            "expected a one-byte buffer (itemsize 1)",
        ));
    }
    if buffer.is_c_contiguous() {
        let ptr = buffer.buf_ptr().cast::<u8>();
        let len = buffer.len_bytes();
        // SAFETY: buffer is held for this scope, C-contiguous one-byte items,
        // and `len_bytes` is the authoritative length.
        let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
        return f(bytes);
    }
    let owned = buffer.to_vec(py)?;
    // One-byte element: reinterpret storage as u8 without changing layout.
    // SAFETY: `T` is u8 or i8 (item_size == 1); same length in bytes.
    let bytes = unsafe { std::slice::from_raw_parts(owned.as_ptr().cast::<u8>(), owned.len()) };
    f(bytes)
}

/// Collect a Python sequence/iterable of extractable items without trusting
/// `__len__` for capacity (the D01 keystone: never `Vec::with_capacity` from
/// a lying length). One pass: iterate, extract, fallible push.
pub(crate) fn collect_extracted_sequence<T>(
    value: &Bound<'_, PyAny>,
    what: &str,
) -> PyResult<Vec<T>>
where
    for<'a, 'py> T: FromPyObject<'a, 'py>,
{
    if let Ok(iter) = value.try_iter() {
        let mut out = Vec::new();
        for item in iter {
            let item = item?;
            let parsed: T = item.extract().map_err(|_| {
                PyTypeError::new_err(format!("{what} elements have the wrong type"))
            })?;
            try_push(&mut out, parsed)?;
        }
        return Ok(out);
    }
    // Sequence without iterator: walk by index with a clamped length *hint*.
    if let Ok(sequence) = value.cast::<PySequence>() {
        let reported = sequence.len()?;
        let mut out = try_vec_with_capacity_hint(reported)?;
        for index in 0..reported {
            let item = sequence.get_item(index)?;
            let parsed: T = item.extract().map_err(|_| {
                PyTypeError::new_err(format!("{what} elements have the wrong type"))
            })?;
            try_push(&mut out, parsed)?;
        }
        return Ok(out);
    }
    Err(PyTypeError::new_err(format!(
        "{what} must be a sequence or iterable"
    )))
}

/// Collect `u64` ids for pickle reconstructors (cells, coverages, …).
pub(crate) fn collect_u64_sequence(value: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<u64>> {
    collect_extracted_sequence(value, what)
}

/// Collect `usize` handles for pickle reconstructors (spatial index live rows).
pub(crate) fn collect_usize_sequence(value: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<usize>> {
    collect_extracted_sequence(value, what)
}

/// Collect `i64` CSR offsets / int groups for pickle reconstructors.
pub(crate) fn collect_i64_sequence(value: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<i64>> {
    collect_extracted_sequence(value, what)
}

/// THE mandatory entry for every Python-iterable → `Vec` boundary.
///
/// `__len__` is a clamped **hint** only; every element is grown via
/// [`try_push`]. Callers that need a mapped type pass a closure; the identity
/// map (`|item| Ok(item)`) materializes the raw `Bound` items.
///
/// Prefer this over `iter.collect::<PyResult<Vec<_>>>()` or bare `Vec::push`
/// loops — those grow infallibly until Rust aborts on capacity overflow / OOM.
/// There is no bare-stream element ceiling: a finite generator of N elements
/// succeeds the same way as a list of N. The retained output grows through
/// fallible reservation; `map` may itself allocate while constructing an item.
pub(crate) fn collect_py_iter<'py, T>(
    values: &Bound<'py, PyAny>,
    mut map: impl FnMut(Bound<'py, PyAny>) -> PyResult<T>,
) -> PyResult<Vec<T>> {
    let mut out = Vec::new();
    // Length is a clamped *hint* only — never a validity gate.
    if let Ok(hint) = values.len() {
        try_reserve_hint(&mut out, hint)?;
    }
    for item in values.try_iter()? {
        // Reserve before mapping. Mapping one iterable member commonly makes
        // several small Rust/Python allocations (a Polygon hole builds its
        // coordinate columns, for example). If `out` is at capacity, doing
        // that work first can hit an infallible allocation under RLIMIT_AS
        // before `try_push` has a chance to turn the next growth into
        // `MemoryError`. The reservation is otherwise identical and remains
        // purely fallible; only its order is the security boundary.
        try_reserve_next(&mut out)?;
        out.push(map(item?)?);
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
    mut map: impl FnMut(Bound<'py, PyAny>) -> PyResult<T>,
    on_len: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<T>> {
    let mut out = try_vec_with_capacity(expected)?;
    let mut iter = values.try_iter()?;
    for _ in 0..=expected {
        let Some(item) = iter.next() else {
            break;
        };
        try_push(&mut out, map(item?)?)?;
    }
    if out.len() != expected {
        return Err(on_len(out.len()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
