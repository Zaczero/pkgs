//! Python protocol admission and signal-aware collection.
use pyo3::exceptions::PyTypeError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyIterator, PySequence, PyString};
/// Check pending Python signals often enough that a native loop over a Python
/// iterator remains interruptible without paying for a signal check on every
/// item.
pub const SIGNAL_STRIDE: usize = 64;
pub fn check_signals(py: Python<'_>, item: usize) -> PyResult<()> {
    if item.is_multiple_of(SIGNAL_STRIDE) {
        py.check_signals()?;
    }
    Ok(())
}
/// Check pending Python signals from a native loop, attaching only at the
/// signal-check stride. Propagate a pending Python exception unchanged.
#[expect(
    clippy::redundant_closure_for_method_calls,
    reason = "the method item is not higher-ranked over the attachment lifetime"
)]
pub fn check_detached_signals(item: usize) -> PyResult<()> {
    if item.is_multiple_of(SIGNAL_STRIDE) {
        Python::attach(|py| py.check_signals())?;
    }
    Ok(())
}
/// Acquire a Python iterator while distinguishing a plain non-iterable from
/// an iterable provider whose protocol raised. The latter exception is part
/// of the caller's input and must not be rewritten as a type classification.
pub fn iter_or_else<'py, E>(value: &Bound<'py, PyAny>, error: E) -> PyResult<Bound<'py, PyIterator>>
where
    E: FnOnce() -> PyErr,
{
    try_iter(value)?.ok_or_else(error)
}
/// Whether the type supplies Python's iterator or legacy sequence protocol.
///
/// Instance attributes named ``__iter__`` and ``__getitem__`` do not
/// participate in implicit special-method lookup.
pub fn has_iter(value: &Bound<'_, PyAny>) -> bool {
    type_has_slot(value, ffi::Py_tp_iter) || type_has_slot(value, ffi::Py_sq_item)
}
/// Acquire a Python iterator, returning `None` only when the value has neither
/// the iterator nor legacy sequence protocol. Errors raised by a supplied
/// protocol are returned unchanged.
pub fn try_iter<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyIterator>>> {
    if has_iter(value) {
        value.try_iter().map(Some)
    } else {
        Ok(None)
    }
}
fn type_has_slot(value: &Bound<'_, PyAny>, slot: std::ffi::c_int) -> bool {
    let ty = value.get_type();
    // SAFETY: `ty` is a live Python type and `slot` is one of the public,
    // supported slot identifiers used by the callers below.
    unsafe { !ffi::PyType_GetSlot(ty.as_type_ptr(), slot).is_null() }
}
/// Read Python's size only when the type supplies a length slot.
///
/// Missing length is normal for a one-shot iterator. Calling `len()` after
/// classification preserves the provider's descriptor behavior and exception.
pub fn optional_len(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    if !type_has_slot(value, ffi::Py_mp_length) && !type_has_slot(value, ffi::Py_sq_length) {
        return Ok(None);
    }
    value.len().map(Some)
}
/// Reserve from Python's exact length contract without limiting iteration.
///
/// Call after acquiring the iterator so an iterator-provider exception keeps
/// precedence over a `__len__` exception.
pub fn reserve<T>(values: &Bound<'_, PyAny>, output: &mut Vec<T>) -> PyResult<()> {
    if let Some(len) = optional_len(values)? {
        output.reserve(len);
    }
    Ok(())
}
/// Collect sequence elements once for classification and materialization.
pub fn collect_owned(sequence: &Bound<'_, PySequence>) -> PyResult<Vec<Py<PyAny>>> {
    collect(sequence.as_any(), |item| Ok(item.unbind()))
}
/// Collect Python strings while preserving provider and type errors.
pub fn collect_strings(values: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<String>> {
    let iter = iter_or_else(values, || {
        PyTypeError::new_err(format!("{what} must be an iterable of strings"))
    })?;
    let mut out = Vec::new();
    reserve(values, &mut out)?;
    for (index, item) in iter.enumerate() {
        let item = item?;
        check_signals(values.py(), index)?;
        let text = item
            .cast::<PyString>()
            .map_err(|_| PyTypeError::new_err(format!("{what} elements have the wrong type")))?;
        out.push(text.to_str()?.to_owned());
    }
    Ok(out)
}
/// Collect Python items with optional mapping and exact-length preallocation.
///
/// There is no bare-stream element ceiling: a finite generator of N elements
/// has the same validity as a list of N. Provider and mapping errors stop the
/// iteration and release retained output; native allocator exhaustion aborts.
pub fn collect<'py, T, F>(values: &Bound<'py, PyAny>, map: F) -> PyResult<Vec<T>>
where
    F: FnMut(Bound<'py, PyAny>) -> PyResult<T>,
{
    let iter = values.try_iter()?;
    collect_from(values, iter, map)
}
/// Collect from an iterator that the caller already acquired. Keeping the
/// acquisition outside the collector lets callers distinguish a non-iterable
/// scalar from an iterator provider that failed during acquisition, without a
/// destructive second probe.
pub fn collect_from<'py, T, F>(
    values: &Bound<'py, PyAny>,
    iter: Bound<'py, PyIterator>,
    mut map: F,
) -> PyResult<Vec<T>>
where
    F: FnMut(Bound<'py, PyAny>) -> PyResult<T>,
{
    let mut out = Vec::new();
    reserve(values, &mut out)?;
    for (index, item) in iter.enumerate() {
        let item = item?;
        check_signals(values.py(), index)?;
        out.push(map(item)?);
    }
    Ok(out)
}
/// Visit an already-acquired Python iterator with the shared signal cadence.
pub fn visit_from<'py, F>(
    values: &Bound<'py, PyAny>,
    iter: Bound<'py, PyIterator>,
    mut visit: F,
) -> PyResult<()>
where
    F: FnMut(Bound<'py, PyAny>) -> PyResult<()>,
{
    for (index, item) in iter.enumerate() {
        let item = item?;
        check_signals(values.py(), index)?;
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
pub fn collect_exact<'py, T, F, E>(
    values: &Bound<'py, PyAny>,
    expected: usize,
    map: F,
    on_len: E,
) -> PyResult<Vec<T>>
where
    F: FnMut(Bound<'py, PyAny>) -> PyResult<T>,
    E: FnOnce(usize) -> PyErr,
{
    let iter = values.try_iter()?;
    collect_exact_from(values, iter, expected, map, on_len)
}
/// Exact collector sibling for callers that already acquired the iterator.
pub fn collect_exact_from<'py, T, F, E>(
    values: &Bound<'py, PyAny>,
    mut iter: Bound<'py, PyIterator>,
    expected: usize,
    mut map: F,
    on_len: E,
) -> PyResult<Vec<T>>
where
    F: FnMut(Bound<'py, PyAny>) -> PyResult<T>,
    E: FnOnce(usize) -> PyErr,
{
    let mut out = Vec::with_capacity(expected);
    for index in 0..expected {
        check_signals(values.py(), index)?;
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
pub fn collect_at_most_from<'py, T, F, E>(
    values: &Bound<'py, PyAny>,
    mut iter: Bound<'py, PyIterator>,
    maximum: usize,
    mut map: F,
    on_excess: E,
) -> PyResult<Vec<T>>
where
    F: FnMut(Bound<'py, PyAny>) -> PyResult<T>,
    E: FnOnce(usize) -> PyErr,
{
    let mut out = Vec::new();
    for index in 0..maximum {
        check_signals(values.py(), index)?;
        let Some(item) = iter.next() else {
            return Ok(out);
        };
        out.push(map(item?)?);
    }
    check_signals(values.py(), maximum)?;
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
