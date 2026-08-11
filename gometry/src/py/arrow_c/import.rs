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

use pyo3::exceptions::{PyMemoryError, PyOSError};

use crate::Frame;
use crate::py::arrow_c::{
    ArrowArray, ArrowArrayStream, ArrowSchema, Bound, ImportedStreamGuard, ParseError, Py, PyAny,
    PyAnyMethods as _, PyCapsule, PyErr, PyGeometryArray, PyResult, PyTuple, PyTupleMethods as _,
    PyTypeError, Python, array_capsule_name, c_char, capsule_destructor, crs_arc, empty_array, ffi,
    owned_capsule, release_imported, stream_capsule_name, used_array_capsule_name,
};

pub(crate) fn geometries_from_arrow_c(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<Py<PyAny>>> {
    // Pure C export collapses ExtensionType + field metadata into one map
    // (field wins). Dual-source ExtensionType/field conflicts are reconciled
    // only on the PyArrow-direct path where both carriers are still visible
    // (`geometries_from_pyarrow` / `read_all`). Do NOT walk globals, closures,
    // or wrapper object graphs here — that invents conflicts from objects the
    // provider never exports (R05).
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
        return crate::py::arrow::geometries_from_native_capsules(
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

pub(crate) fn owned_array_capsule(py: Python<'_>, array: ArrowArray) -> PyResult<Py<PyAny>> {
    owned_capsule(
        py,
        Box::new(array),
        array_capsule_name(),
        imported_array_capsule_destructor,
        release_imported_array_callback,
    )
}

unsafe extern "C" fn imported_array_capsule_destructor(capsule: *mut ffi::PyObject) {
    // SAFETY: imported array capsules use the standard Arrow capsule names.
    unsafe {
        capsule_destructor::<ArrowArray>(
            capsule,
            array_capsule_name(),
            used_array_capsule_name(),
            release_imported_array_callback,
        );
    }
}

unsafe extern "C" fn release_imported_array_callback(array: *mut ArrowArray) {
    // SAFETY: capsule destructor owns the shell for this one-shot release.
    unsafe { release_imported_array(array) }
}

pub(crate) fn capsule_pointer<T>(
    capsule: &Bound<'_, PyAny>,
    name: *const c_char,
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
pub(crate) unsafe fn release_imported_array(array: *mut ArrowArray) {
    // SAFETY: forwarded from caller.
    unsafe { release_imported(array) }
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
        pairs
            .try_reserve(pair_count)
            .map_err(|_| ParseError::new_err("Arrow C schema metadata is too large to allocate"))?;
        for _ in 0..pair_count {
            let key_len = read_metadata_len(&mut cursor)?;
            let key = std::slice::from_raw_parts(cursor, key_len)
                .to_vec()
                .into_boxed_slice();
            cursor = cursor.add(key_len);
            let value_len = read_metadata_len(&mut cursor)?;
            let value = std::slice::from_raw_parts(cursor, value_len)
                .to_vec()
                .into_boxed_slice();
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
    usize::try_from(len)
        .map_err(|_| ParseError::new_err("Arrow C schema metadata has a negative length"))
}

/// Stream import (M2): admit and **decode each batch immediately**, release
/// producer + owned admission buffers for that batch before `get_next`, and
/// accumulate only decoded geometry shapes. Peak intermediate memory tracks
/// one batch, not the whole stream.
pub(crate) fn import_stream(
    py: Python<'_>,
    stream: *mut ArrowArrayStream,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<Py<PyAny>>> {
    let _stream_guard = ImportedStreamGuard::new(stream);
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
        // Design-1: capture schema once into owned storage, release producer
        // schema, classify only the admitted tree. Batches use admitted schema
        // only (no borrowed raw schema capsules).
        let (admitted_schema, classified) =
            match crate::py::arrow_c::admit_and_classify_raw_schema(&raw mut stream_schema) {
                Ok(plan) => plan,
                Err(error) => {
                    // SAFETY: stream_schema is the live producer shell for this path;
                    // no retained borrows. Nested under the stream unsafe block.
                    release_imported_schema(&raw mut stream_schema);
                    return Err(error);
                },
            };
        // stream_schema was moved+released by admit_and_classify.

        // Decoded rows only — never retain per-batch NativeNode/ArrowStorage
        // across get_next. Fallible growth.
        let mut shapes: Vec<crate::geometry::Shape> = Vec::new();
        let mut missing_rows: Vec<usize> = Vec::new();
        let mut row_base = 0_usize;
        let mut batch_crs = classified.crs.clone();
        let mut batch_epoch = classified.epoch;

        loop {
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
            // Zero-row batches use the same owned admission path as nonzero
            // (D18 offset validation lives in decode, not a raw bypass).
            let array_capsule = owned_array_capsule(py, array)?;
            // Admit from array capsule + owned schema (selected-span).
            let geometry = crate::py::arrow_c::native_arrow_from_array_capsule_with_schema(
                py,
                array_capsule.bind(py),
                &admitted_schema,
                classified.struct_child,
            )?;
            let storage = crate::py::arrow::arrow_storage_from_native_geometry(
                geometry.bind(py),
                classified.encoding,
                classified.wkb_offset_width,
                classified.crs.clone(),
                classified.epoch,
            )?;
            // Drop native view (owned buffers) before decoding materializes shapes.
            drop(geometry);
            drop(array_capsule);

            // Decode this batch alone into a GeometryArray, then take shapes.
            let batch =
                crate::py::arrow::geometries_from_arrow_storages(py, vec![storage], None, None)?;
            let batch_bound = batch.bind(py);
            let batch_arr = batch_bound.cast::<crate::PyGeometryArray>()?;
            let borrowed = batch_arr.borrow();
            let batch_len = borrowed.__len__();
            // Collect shapes (missing placeholders included in storage iteration).
            for (i, shape) in borrowed.storage().iter_shapes().enumerate() {
                if borrowed.is_row_missing(i) {
                    crate::try_push(&mut missing_rows, row_base + i)?;
                    crate::try_push(&mut shapes, crate::PyGeometryArray::missing_placeholder())?;
                } else {
                    crate::try_push(&mut shapes, shape.into_owned())?;
                }
            }
            if batch_crs.is_none() {
                batch_crs = borrowed.crs_str().map(str::to_owned);
            }
            if batch_epoch.is_none() {
                batch_epoch = borrowed.epoch();
            }
            row_base = row_base
                .checked_add(batch_len)
                .ok_or_else(|| PyTypeError::new_err("Arrow stream row count overflows"))?;
            // Drop owned batch before next get_next so peak tracks one batch.
            drop(borrowed);
            drop(batch);
        }

        if shapes.is_empty() {
            // Schema already released; use classified frame from owned admission.
            let crs = crate::py::arrow::reconcile_arrow_crs(classified.crs.as_deref(), crs)?;
            let epoch = crate::py::arrow::reconcile_arrow_epoch(classified.epoch, epoch)?;
            return PyGeometryArray::mixed(Vec::new(), Frame::new(crs.map(crs_arc), epoch)?)
                .into_py_any(py)
                .map(Some);
        }
        // Final frame reconcile with caller overrides.
        let crs = crate::py::arrow::reconcile_arrow_crs(batch_crs.as_deref(), crs)?;
        let epoch = crate::py::arrow::reconcile_arrow_epoch(batch_epoch, epoch)?;
        let frame = Frame::new(crs.map(crs_arc), epoch)?;
        // Missing mask is authoritative; placeholders keep storage rectangular.
        let mask = crate::array::MissingMask::from_sparse(shapes.len(), &missing_rows);
        let array = crate::PyGeometryArray::from_shapes(shapes, frame).with_missing_mask(mask);
        array.into_py_any(py).map(Some)
    }
}

/// Map an Arrow C Stream callback's errno-like status to Python while retaining
/// the producer's optional diagnostic. `ENOMEM` is a resource failure, not a
/// type error; all other status values remain errno-bearing `OSError`s.
unsafe fn stream_callback_error(
    py: Python<'_>,
    stream: *mut ArrowArrayStream,
    status: i32,
    fallback: &str,
) -> PyErr {
    // SAFETY: the ImportedStreamGuard keeps this producer stream live until
    // after the error is constructed; get_last_error is optional and checked.
    let detail = unsafe { stream_last_error_detail(stream) }.unwrap_or_else(|| fallback.to_owned());
    // Arrow C Stream follows errno-style return codes. These values are the
    // specified POSIX errno numbers on gometry's Linux targets.
    if status == 12 {
        return PyMemoryError::new_err(format!("Arrow C stream error: {detail}"));
    }
    PyErr::from_type(py.get_type::<PyOSError>(), (status, detail))
}

/// Surface the producer diagnostic without discarding the callback status.
unsafe fn stream_last_error_detail(stream: *mut ArrowArrayStream) -> Option<String> {
    // SAFETY: stream live; get_last_error optional.
    unsafe {
        if let Some(get_last_error) = (*stream).get_last_error {
            let ptr = get_last_error(stream);
            if !ptr.is_null()
                && let Ok(s) = std::ffi::CStr::from_ptr(ptr).to_str()
                && !s.is_empty()
            {
                return Some(format!("Arrow C stream error: {s}"));
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
use pyo3::IntoPyObjectExt as _;
