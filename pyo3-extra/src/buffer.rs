//! Buffer acquisition, validated dimensions and owning byte captures.
use std::ffi::c_int;

use pyo3::buffer::{Element, PyBuffer, PyUntypedBuffer};
use pyo3::exceptions::{PyBufferError, PyOverflowError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
pub mod export;
/// Acquire an exporter only when it advertises the buffer protocol.
///
/// This distinguishes an ordinary non-buffer value from an exporter whose
/// `PyObject_GetBuffer` callback raised an exception.
pub fn get(value: &Bound<'_, PyAny>) -> PyResult<Option<PyUntypedBuffer>> {
    // SAFETY: `value` is a live Python object attached to this call.
    let supports_buffer = unsafe { pyo3::ffi::PyObject_CheckBuffer(value.as_ptr()) } != 0;
    if !supports_buffer {
        return Ok(None);
    }
    PyUntypedBuffer::get(value).map(Some)
}
/// Try a typed view while preserving errors raised by the exporter itself.
///
/// A successful untyped acquisition followed by typed incompatibility keeps
/// the caller's ordinary iterable fallback.
pub fn try_typed<T: Element>(value: &Bound<'_, PyAny>) -> PyResult<Option<PyBuffer<T>>> {
    let Some(buffer) = get(value)? else {
        return Ok(None);
    };
    Ok(buffer.into_typed::<T>().ok())
}
/// Validate exporter-supplied length and shape before using PyO3's derived
/// count, slice, or contiguity helpers.
pub fn item_count<T: Element>(buffer: &PyBuffer<T>) -> PyResult<usize> {
    let byte_len = buffer.len_bytes();
    if byte_len > isize::MAX as usize {
        return Err(PyBufferError::new_err(
            "buffer has a negative or unrepresentable byte length",
        ));
    }

    let item_size = std::mem::size_of::<T>();
    if !byte_len.is_multiple_of(item_size) {
        return Err(PyBufferError::new_err(
            "buffer byte length is not divisible by its item size",
        ));
    }

    // PyO3's `shape()` uses the signed ndim as a slice length. Reject a
    // negative C int before asking it to construct that slice.
    let dimensions = buffer.dimensions();
    if dimensions > c_int::MAX as usize {
        return Err(PyBufferError::new_err(
            "buffer dimension count is negative or unrepresentable",
        ));
    }
    let mut shape_count = 1_usize;
    for &dimension in buffer.shape() {
        if dimension > isize::MAX as usize {
            return Err(PyBufferError::new_err(
                "buffer shape contains a negative dimension",
            ));
        }
        shape_count = shape_count
            .checked_mul(dimension)
            .ok_or_else(|| PyBufferError::new_err("buffer shape product overflows"))?;
    }
    let item_count = byte_len
        .checked_div(item_size)
        .ok_or_else(|| PyBufferError::new_err("buffer element size must be nonzero"))?;
    if shape_count != item_count {
        return Err(PyBufferError::new_err(
            "buffer shape does not match its byte length",
        ));
    }
    if item_count != 0 && buffer.buf_ptr().is_null() {
        return Err(PyBufferError::new_err("buffer has a null data pointer"));
    }
    validate_buffer_stride_span(buffer)?;
    Ok(item_count)
}
fn validate_buffer_stride_span<T: Element>(buffer: &PyBuffer<T>) -> PyResult<()> {
    let shape = buffer.shape();
    let strides = buffer.strides();
    if shape.len() != strides.len() {
        return Err(PyBufferError::new_err(
            "buffer stride count does not match its dimensions",
        ));
    }
    if shape.contains(&0) {
        return Ok(());
    }

    let mut minimum = 0_isize;
    let mut maximum = 0_isize;
    for (&dimension, &stride) in shape.iter().zip(strides) {
        let steps = isize::try_from(dimension.wrapping_sub(1))
            .map_err(|_| PyOverflowError::new_err("buffer stride span overflows"))?;
        let offset = steps
            .checked_mul(stride)
            .ok_or_else(|| PyOverflowError::new_err("buffer stride span overflows"))?;
        if offset < 0 {
            minimum = minimum
                .checked_add(offset)
                .ok_or_else(|| PyOverflowError::new_err("buffer stride span overflows"))?;
        } else {
            maximum = maximum
                .checked_add(offset)
                .ok_or_else(|| PyOverflowError::new_err("buffer stride span overflows"))?;
        }
    }
    let item_size = isize::try_from(std::mem::size_of::<T>())
        .map_err(|_| PyOverflowError::new_err("buffer stride span overflows"))?;
    maximum
        .checked_sub(minimum)
        .and_then(|span| span.checked_add(item_size))
        .ok_or_else(|| PyOverflowError::new_err("buffer stride span overflows"))?;
    Ok(())
}
/// Copy a one-byte Python buffer into exclusive Rust storage. This is the
/// owning counterpart to [`with_bytes`].
/// The source producer must remain quiescent until the copy returns; owning
/// the result does not synchronize the source read.
pub fn copy_bytes(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    match copy_exported_bytes(value) {
        Ok(bytes) => Ok(bytes),
        Err(CopyError::NotOneByte) => Err(PyTypeError::new_err(
            "expected a one-byte buffer (bytes, bytearray, memoryview, or array.array)",
        )),
        Err(CopyError::PythonError(error)) => Err(error),
    }
}
/// Distinguish an incompatible item format from a failing buffer provider.
#[derive(Debug)]
pub enum CopyError {
    NotOneByte,
    PythonError(PyErr),
}
pub fn copy_exported_bytes(value: &Bound<'_, PyAny>) -> std::result::Result<Vec<u8>, CopyError> {
    let buffer = get(value)
        .map_err(CopyError::PythonError)?
        .ok_or(CopyError::NotOneByte)?;
    if let Ok(buffer) = buffer.as_typed::<u8>() {
        let item_count = item_count(buffer).map_err(CopyError::PythonError)?;
        if item_count == 0 {
            return Ok(Vec::new());
        }
        return buffer.to_vec(value.py()).map_err(CopyError::PythonError);
    }
    if let Ok(buffer) = buffer.as_typed::<i8>() {
        let item_count = item_count(buffer).map_err(CopyError::PythonError)?;
        if item_count == 0 {
            return Ok(Vec::new());
        }
        let signed = buffer.to_vec(value.py()).map_err(CopyError::PythonError)?;
        let (ptr, len, capacity) = signed.into_raw_parts();
        // SAFETY: i8 and u8 have the same layout and every byte is initialized.
        return Ok(unsafe { Vec::from_raw_parts(ptr.cast(), len, capacity) });
    }
    Err(CopyError::NotOneByte)
}
/// True when ``value`` exports a one-byte buffer (signed or unsigned item
/// format): ``bytes``/``bytearray``/``memoryview``/``array.array('B'|'b')``.
pub fn is_bytes(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.is_instance_of::<PyBytes>() {
        return Ok(true);
    }
    let Some(buffer) = get(value)? else {
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
/// known-immutable ``bytes``. Mutable
/// carriers (`bytearray`, writable `memoryview`, NumPy, …) are **copied**
/// before parse. A `PyBuffer` pins the allocation but does not stop another
/// thread mutating contents, so the producer must remain quiescent until the
/// copy returns; the resulting parse borrows only the owned copy.
pub fn with_bytes<R, F>(value: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[u8]) -> PyResult<R>,
{
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return f(bytes.as_bytes());
    }
    let owned = copy_bytes(value)?;
    f(&owned)
}
