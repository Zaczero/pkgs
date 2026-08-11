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

use pyo3::exceptions::{PyMemoryError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PySequence;

/// Soft upper bound for a single **initial** reservation from an external
/// length *hint* (`__len__`, etc.). Large enough to avoid thrash on typical
/// batch sizes; small enough that a lying `sys.maxsize` / `i64::MAX` hint
/// cannot force a multi-gigabyte empty `Vec` before the first item is
/// yielded. Growth after the first item stays fallible via `try_reserve` /
/// `try_reserve_next` on actual observed elements — never promote a hint to
/// an exact allocation or iteration count (AGENTS carrier 3).
pub(crate) const MAX_RESERVE_HINT: usize = 1 << 12; // 4096 elements

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

/// Message for outer-vector growth failure on untrusted streams.
pub(crate) const GROW_SEQUENCE_OOM: &str = "failed to grow sequence buffer from untrusted input";

/// Message for fallible owned-string construction on untrusted streams.
pub(crate) const STRING_ALLOC_OOM: &str = "failed to allocate string from untrusted input";

/// Probe whether `vec` needs a growth reservation; fallible, no `PyErr` yet.
///
/// Returns `Err(())` on allocator failure. Callers that already hold retained
/// untrusted output **must drop that output before** converting to
/// [`PyMemoryError`] — boxing a `PyErr` while the heap is exhausted aborts
/// with `memory allocation of N bytes failed` instead of a catchable error.
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
/// On OOM the **item is dropped** with the vector contents before the
/// `PyMemoryError` is constructed, so exception boxing has address-space
/// headroom under `RLIMIT_AS`.
pub(crate) fn try_push<T>(vec: &mut Vec<T>, item: T) -> PyResult<()> {
    if try_reserve_next_raw(vec).is_err() {
        drop(item);
        vec.clear();
        // Return capacity to the allocator when possible so `PyErr` boxing can
        // succeed under a tight RLIMIT_AS.
        vec.shrink_to_fit();
        return Err(PyMemoryError::new_err(GROW_SEQUENCE_OOM));
    }
    vec.push(item);
    Ok(())
}

/// Fallibly copy a UTF-8 slice into an owned [`String`].
///
/// `Err(())` means the allocator refused the reservation — **do not** build a
/// `PyErr` until any large retained buffers that exhausted memory have been
/// dropped (see [`collect_py_iter`]). Use [`string_alloc_error`] after free.
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

/// [`PyMemoryError`] for [`try_string_from_str`] failure — call only after
/// releasing retained untrusted output that exhausted the heap.
pub(crate) fn string_alloc_error() -> PyErr {
    PyMemoryError::new_err(STRING_ALLOC_OOM)
}

/// [`PyMemoryError`] for outer-vector growth failure — call only after
/// releasing retained untrusted output.
pub(crate) fn grow_sequence_error() -> PyErr {
    PyMemoryError::new_err(GROW_SEQUENCE_OOM)
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
    // `to_vec` → PyBuffer_ToContiguous into exclusively owned Rust storage.
    let owned = buffer.to_vec(py)?;
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
    let signed = buffer.to_vec(py)?;
    // Bit-preserving i8 → u8 on exclusive storage (no alias into Python memory).
    let owned: Vec<u8> = signed.into_iter().map(|b| b as u8).collect();
    f(&owned)
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
/// ## Drop retained output before boxing `PyErr`
///
/// Under a tight address-space cap the retained `Vec` may be what exhausted
/// the heap. Constructing `PyMemoryError` (a Rust `PyErr` box) while that
/// `Vec` still holds every item aborts instead of raising. This collector
/// drops `out` before any OOM `PyErr` is built; maps that build `PyErr` on
/// their own OOM path must use [`try_string_from_str`]'s `Err(())` form (or
/// equivalent) so the collector can free first — see
/// [`crate::py::crs::functions_config`] for the search-path loop.
pub(crate) fn collect_py_iter<'py, T>(
    values: &Bound<'py, PyAny>,
    mut map: impl FnMut(Bound<'py, PyAny>) -> PyResult<T>,
) -> PyResult<Vec<T>> {
    let mut out = Vec::new();
    // Length is a clamped *hint* only — never a validity gate.
    if let Ok(hint) = values.len() {
        try_reserve_hint(&mut out, hint)?;
    }
    let mut iter = values.try_iter()?;
    loop {
        let item = match iter.next() {
            None => break,
            Some(Ok(item)) => item,
            Some(Err(err)) => {
                drop(out);
                return Err(err);
            },
        };
        // Reserve a slot for `T` before mapping so outer growth is fallible.
        // Free retained items *before* boxing `PyMemoryError` (see invariant).
        if try_reserve_next_raw(&mut out).is_err() {
            drop(out);
            return Err(grow_sequence_error());
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
    mut map: impl FnMut(Bound<'py, PyAny>) -> PyResult<T>,
    on_len: impl FnOnce(usize) -> PyErr,
) -> PyResult<Vec<T>> {
    let mut out = try_vec_with_capacity(expected)?;
    let mut iter = values.try_iter()?;
    for _ in 0..expected {
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
