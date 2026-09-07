#![allow(
    clippy::absolute_paths,
    reason = "stream admit/decode loop is intentionally linear"
)]
#![allow(
    clippy::too_many_lines,
    clippy::needless_question_mark,
    reason = "stream admit/decode loop is intentionally linear"
)]

use std::ptr;

use pyo3::exceptions::{PyMemoryError, PyOSError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyCapsule, PyTuple};

use crate::py::arrow::NativeArrowAccumulator;
use crate::py::arrow_c::{
    ArrowArrayStream, ArrowSchema, NativeArrowClassification, empty_array, ffi, release_imported,
    stream_capsule_name,
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
        return import_stream(py, stream, crs, epoch);
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

/// # Safety
/// See [`release_imported`].
pub(crate) unsafe fn release_imported_schema(schema: *mut ArrowSchema) {
    // SAFETY: forwarded from caller.
    unsafe { release_imported(schema) }
}

/// # Safety
/// See [`release_imported`].
pub(crate) unsafe fn release_imported_stream(stream: *mut ArrowArrayStream) {
    // SAFETY: forwarded from caller.
    unsafe { release_imported(stream) }
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
        let mut pairs = Vec::new();
        crate::boundary::try_reserve_vec(&mut pairs, pair_count).map_err(PyErr::from)?;
        for _ in 0..pair_count {
            let key_len = read_metadata_len(&mut cursor)?;
            let key = copy_metadata_bytes(std::slice::from_raw_parts(cursor, key_len), &mut pairs)?;
            cursor = cursor.add(key_len);
            let value_len = read_metadata_len(&mut cursor)?;
            let value =
                copy_metadata_bytes(std::slice::from_raw_parts(cursor, value_len), &mut pairs)?;
            cursor = cursor.add(value_len);
            pairs.push((key, value));
        }
        Ok(pairs)
    }
}

fn copy_metadata_bytes(bytes: &[u8], retained: &mut OwnedMetadataPairs) -> PyResult<Box<[u8]>> {
    let mut output = Vec::new();
    if let Err(error) = crate::boundary::try_reserve_vec(&mut output, bytes.len()) {
        *retained = Vec::new();
        return Err(error.into());
    }
    output.extend_from_slice(bytes);
    Ok(output.into_boxed_slice())
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
/// Only decoded shapes and row metadata survive across callbacks.
pub(crate) fn import_stream(
    py: Python<'_>,
    stream: *mut ArrowArrayStream,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<Py<PyAny>>> {
    let _stream_guard = crate::py::arrow_c::ImportedStreamGuard::new(stream);
    if stream.is_null() {
        return Ok(None);
    }
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
        if schema_status != 0 {
            return Err(stream_callback_error(
                py,
                stream,
                schema_status,
                "Arrow C stream failed to export its schema",
            ));
        }
        let (admitted_schema, classified) =
            match crate::py::arrow_c::admit_and_classify_raw_schema(&raw mut stream_schema) {
                Ok(plan) => plan,
                Err(error) => {
                    // SAFETY: stream_schema is the live producer shell when
                    // schema admission failed before it could take ownership.
                    if stream_schema.release.is_some() {
                        release_imported_schema(&raw mut stream_schema);
                    }
                    return Err(error);
                },
            };
        let (classification, struct_child) = NativeArrowClassification::from_schema(&classified);
        let mut accumulator = NativeArrowAccumulator::new();
        accumulator.admit_classification(&classification)?;
        let mut batch_index: usize = 0;

        loop {
            crate::check_python_signals(py, batch_index)?;
            let mut array = empty_array();
            let next_status = get_next(stream, &raw mut array);
            if next_status != 0 {
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
    // SAFETY: the stream guard keeps this producer live until the error is
    // constructed; get_last_error is optional and checked.
    let detail = unsafe { stream_last_error_detail(stream) }.unwrap_or_else(|| fallback.to_owned());
    if status == 12 {
        return PyMemoryError::new_err(format!("Arrow C stream error: {detail}"));
    }
    PyErr::from_type(py.get_type::<PyOSError>(), (status, detail))
}

unsafe fn stream_last_error_detail(stream: *mut ArrowArrayStream) -> Option<String> {
    // SAFETY: stream live; get_last_error optional.
    unsafe {
        if let Some(get_last_error) = (*stream).get_last_error {
            let ptr = get_last_error(stream);
            if !ptr.is_null()
                && let Ok(s) = std::ffi::CStr::from_ptr(ptr).to_str()
                && !s.is_empty()
            {
                return Some(s.to_owned());
            }
        }
    }
    None
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
