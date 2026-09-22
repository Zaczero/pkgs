#![expect(
    clippy::absolute_paths,
    reason = "stream admit/decode loop is intentionally linear"
)]

use std::ptr;

use libc::ENOMEM;
use pyo3::exceptions::{PyOSError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyString, PyTuple};

use crate::py::arrow::NativeArrowAccumulator;
use crate::py::arrow_c::{
    ArrowArrayStream, ArrowReleaseGuard, ArrowSchema, MovedArrowShell, NativeArrowClassification,
    empty_array, ffi, stream_capsule_name,
};

pub(crate) fn geometries_from_arrow_c(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<Py<PyAny>>> {
    // Pure C export has only its exported schema; do not inspect wrapper
    // objects or globals to invent field/type conflicts.
    if value.hasattr("__arrow_c_array__")? {
        let capsules = value.call_method1("__arrow_c_array__", (py.None(),))?;
        let tuple = capsules.cast::<PyTuple>()?;
        if tuple.len() != 2 {
            return Err(PyTypeError::new_err(
                "__arrow_c_array__ must return (schema_capsule, array_capsule)",
            ));
        }
        let schema_capsule = tuple.get_item(0)?;
        let array_capsule = tuple.get_item(1)?;
        return geometries_from_native_capsules_classified(
            py,
            &schema_capsule,
            &array_capsule,
            crs,
            epoch,
        )
        .map(Some);
    }
    if value.hasattr("__arrow_c_stream__")? {
        let capsule = value.call_method1("__arrow_c_stream__", (py.None(),))?;
        let stream = capsule_pointer::<ArrowArrayStream>(&capsule, stream_capsule_name())?;
        // SAFETY: `capsule_pointer` checked the stream capsule name, and the
        // capsule keeps its stream alive while it is borrowed here.
        return unsafe { import_stream(py, stream, crs, epoch) };
    }
    Ok(None)
}

pub(crate) fn geometries_from_native_capsules_classified(
    py: Python<'_>,
    schema_capsule: &Bound<'_, PyAny>,
    array_capsule: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let (batch, classified) =
        crate::py::arrow_c::native_arrow_batch_from_capsules(schema_capsule, array_capsule)?;
    let (classification, _) = NativeArrowClassification::from_schema(&classified);
    let mut accumulator = NativeArrowAccumulator::new();
    accumulator.admit_classification(&classification)?;
    let decoded = py
        .detach(move || batch.decode())
        .map_err(crate::py::arrow_c::NativeArrowDecodeError::into_py_err)?;
    accumulator.push_decoded(decoded)?;
    accumulator.finish(py, crs, epoch)
}

pub(crate) fn capsule_pointer<T>(
    capsule: &Bound<'_, PyAny>,
    name: *const std::ffi::c_char,
) -> PyResult<*mut T> {
    if !capsule.is_instance_of::<PyCapsule>() {
        return Err(PyTypeError::new_err("expected Arrow PyCapsule"));
    }
    // SAFETY: CPython validates the capsule name and returns the stored
    // pointer or sets an exception. We never dereference a null result.
    let ptr = unsafe { ffi::PyCapsule_GetPointer(capsule.as_ptr(), name).cast::<T>() };
    if ptr.is_null() {
        return Err(PyErr::fetch(capsule.py()));
    }
    Ok(ptr)
}

type OwnedMetadataPairs = Vec<(Box<[u8]>, Box<[u8]>)>;

/// Copy every Arrow schema metadata key/value pair into owned storage.
///
/// # Safety
///
/// `schema.metadata` is live and quiescent for the full copy; no producer
/// release may run concurrently. Only the owned pairs may escape.
pub(crate) unsafe fn schema_metadata_pairs_owned(
    schema: &ArrowSchema,
) -> PyResult<OwnedMetadataPairs> {
    if schema.metadata.is_null() {
        return Ok(Vec::new());
    }
    // SAFETY: Arrow C schema metadata is a contiguous native-endian key/value
    // blob owned by the producer while the schema release callback is live.
    unsafe {
        let mut cursor = schema.metadata.cast::<u8>();
        let pair_count = read_metadata_len(&mut cursor)?;
        let mut pairs = Vec::with_capacity(pair_count);
        for _ in 0..pair_count {
            let key_len = read_metadata_len(&mut cursor)?;
            let key = Box::from(std::slice::from_raw_parts(cursor, key_len));
            cursor = cursor.add(key_len);
            let value_len = read_metadata_len(&mut cursor)?;
            let value = Box::from(std::slice::from_raw_parts(cursor, value_len));
            cursor = cursor.add(value_len);
            pairs.push((key, value));
        }
        Ok(pairs)
    }
}

const unsafe fn read_metadata_i32(cursor: &mut *const u8) -> i32 {
    let mut bytes = [0_u8; 4];
    // SAFETY: the caller guarantees at least four readable bytes at `cursor`.
    unsafe {
        ptr::copy_nonoverlapping(*cursor, bytes.as_mut_ptr(), bytes.len());
        *cursor = cursor.add(bytes.len());
    }
    i32::from_ne_bytes(bytes)
}

unsafe fn read_metadata_len(cursor: &mut *const u8) -> PyResult<usize> {
    // SAFETY: forwarded from `schema_metadata_value`.
    let len = unsafe { read_metadata_i32(cursor) };
    usize::try_from(len).map_err(|_| {
        crate::py::errors::ParseError::new_err("Arrow C schema metadata has a negative length")
    })
}

/// Stream import admits and decodes one batch before requesting the next.
/// Only canonical packed payloads (or explicit mixed rows) and row metadata
/// survive across callbacks.
///
/// # Safety
///
/// `stream` must be null or point to a writable, ABI-conforming
/// `ArrowArrayStream` that stays valid for the duration of the call; the
/// import takes ownership of its release callback.
pub(crate) unsafe fn import_stream(
    py: Python<'_>,
    stream: *mut ArrowArrayStream,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<Py<PyAny>>> {
    if stream.is_null() {
        return Ok(None);
    }
    // The capsule normally remains the fallback owner if moving the stream
    // shell cannot allocate. Keep an explicit stack owner as well so direct
    // native callers have the same failure-path guarantee.
    // SAFETY: the caller supplies a live, writable stream (checked non-null).
    let _stream_guard = unsafe { ArrowReleaseGuard::from_raw(stream) }?;
    // Consume the capsule-owned stream shell before inspecting callback
    // pointers. The original capsule remains only as an inert box whose
    // release slot is NULL, so reentrant or repeated imports cannot callback.
    // SAFETY: the same live stream; the guard above yields to the moved owner.
    let mut stream_owner = unsafe { MovedArrowShell::take(stream) }?;
    let stream = stream_owner.as_mut_ptr();
    // SAFETY: a valid stream provides function pointers while unreleased.
    unsafe {
        let Some(get_schema) = (*stream).get_schema else {
            return Ok(None);
        };
        let Some(get_next) = (*stream).get_next else {
            return Ok(None);
        };
        let mut stream_schema = empty_schema();
        let schema_status = get_schema(stream, &raw mut stream_schema);
        // Install this before inspecting status or attempting schema
        // admission. If the producer initialized a live output and a later
        // allocation fails, this stack owner releases it exactly once.
        let schema_guard = ArrowReleaseGuard::from_raw(&raw mut stream_schema)?;
        if schema_status != 0 {
            return Err(stream_callback_error(
                py,
                stream,
                schema_status,
                "Arrow C stream failed to export its schema",
            ));
        }
        let (admitted_schema, classified) =
            crate::py::arrow_c::admit_and_classify_raw_schema(schema_guard.as_ptr())?;
        let (classification, struct_child) = NativeArrowClassification::from_schema(&classified);
        let mut accumulator = NativeArrowAccumulator::new();
        accumulator.admit_classification(&classification)?;
        let mut batch_index: usize = 0;

        loop {
            crate::check_python_signals(py, batch_index)?;
            let mut array = empty_array();
            let next_status = get_next(stream, &raw mut array);
            if next_status != 0 {
                // A producer may have populated an output before reporting
                // failure. Install the non-allocating guard before consulting
                // its diagnostic.
                let _array_guard = ArrowReleaseGuard::from_raw(&raw mut array)?;
                return Err(stream_callback_error(
                    py,
                    stream,
                    next_status,
                    "Arrow C stream failed to export its next array",
                ));
            }
            if array.release.is_none() {
                break;
            }
            // Move and release the producer shell before detaching.  The batch
            // owns independent copies of every selected span.
            let batch = crate::py::arrow_c::native_arrow_batch_from_raw_array(
                &raw mut array,
                &admitted_schema,
                classification.clone(),
                struct_child,
            )?;
            let decoded = py
                .detach(move || batch.decode())
                .map_err(crate::py::arrow_c::NativeArrowDecodeError::into_py_err)?;
            accumulator.push_decoded(decoded)?;
            batch_index = batch_index.wrapping_add(1);
        }
        accumulator.finish(py, crs, epoch).map(Some)
    }
}

/// Map an Arrow C Stream callback status to Python while retaining the
/// producer's optional diagnostic.
unsafe fn stream_callback_error(
    py: Python<'_>,
    stream: *mut ArrowArrayStream,
    status: i32,
    fallback: &str,
) -> PyErr {
    // SAFETY: `MovedArrowShell` keeps the producer stream live and
    // unmodified through this exception construction.
    let detail =
        unsafe { stream.as_mut() }.and_then(|stream| unsafe { stream_last_error_detail(stream) });
    if status == ENOMEM {
        return native_stream_memory_error(py, detail.unwrap_or(fallback));
    }
    let detail = detail.map_or_else(|| fallback.to_owned(), str::to_owned);
    PyErr::from_type(py.get_type::<PyOSError>(), (status, detail))
}

/// Retain the stream producer's ENOMEM diagnostic, or the Python error raised
/// while materializing its message.
fn native_stream_memory_error(py: Python<'_>, detail: &str) -> PyErr {
    let message = match PyString::from_bytes(py, detail.as_bytes()) {
        Ok(message) => message,
        Err(error) => return error,
    };
    // SAFETY: `message` is a live Python string and the public exception type
    // borrows it through the non-stealing setter until `PyErr::fetch` adopts
    // the pending exception.
    unsafe {
        ffi::PyErr_SetObject(ffi::PyExc_MemoryError, message.as_ptr());
    }
    debug_assert!(PyErr::occurred(py));
    PyErr::fetch(py)
}

/// Borrow the provider's last-error string for the duration of the stream
/// guard. The caller must keep the stream live and unmodified while using the
/// returned reference.
unsafe fn stream_last_error_detail(stream: &mut ArrowArrayStream) -> Option<&str> {
    // SAFETY: stream live; get_last_error optional. The exclusive borrow keeps
    // the provider stream unmodified while its returned NUL-terminated text is
    // consumed.
    unsafe {
        let get_last_error = stream.get_last_error?;
        let ptr = get_last_error(std::ptr::from_mut(stream));
        if ptr.is_null() {
            return None;
        }
        let bytes = std::ffi::CStr::from_ptr(ptr).to_bytes();
        if bytes.is_empty() {
            return None;
        }
        std::str::from_utf8(bytes).ok()
    }
}

pub(crate) fn empty_schema() -> ArrowSchema {
    ArrowSchema {
        format: ptr::null(),
        name: ptr::null(),
        metadata: ptr::null(),
        flags: 0,
        n_children: 0,
        children: ptr::null_mut(),
        dictionary: ptr::null_mut(),
        release: None,
        private_data: ptr::null_mut(),
    }
}

#[cfg(test)]
mod tests {
    use pyo3::exceptions::PyMemoryError;
    use pyo3::prelude::*;

    use super::native_stream_memory_error;

    #[test]
    fn stream_memory_error_preserves_provider_detail() {
        Python::initialize();
        Python::attach(|py| {
            let detail = "Arrow producer ran out of memory: π";
            let error = native_stream_memory_error(py, detail);
            assert!(error.is_instance_of::<PyMemoryError>(py));
            assert_eq!(error.value(py).to_string(), detail);
        });
    }
}
