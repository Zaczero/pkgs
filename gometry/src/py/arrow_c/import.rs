#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::ptr;

use crate::Frame;
use crate::py::arrow_c::*;

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

pub(crate) fn owned_schema_capsule(py: Python<'_>, schema: ArrowSchema) -> PyResult<Py<PyAny>> {
    owned_capsule(
        py,
        Box::new(schema),
        schema_capsule_name(),
        imported_schema_capsule_destructor,
        release_imported_schema_callback,
    )
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

unsafe extern "C" fn imported_schema_capsule_destructor(capsule: *mut ffi::PyObject) {
    // SAFETY: imported stream capsules use the standard Arrow capsule names.
    // If a downstream consumer renamed one, the shell is still ours but the
    // release slot should already be NULL.
    unsafe {
        capsule_destructor::<ArrowSchema>(
            capsule,
            schema_capsule_name(),
            used_schema_capsule_name(),
            release_imported_schema_callback,
        );
    }
}

unsafe extern "C" fn imported_array_capsule_destructor(capsule: *mut ffi::PyObject) {
    // SAFETY: see `imported_schema_capsule_destructor`.
    unsafe {
        capsule_destructor::<ArrowArray>(
            capsule,
            array_capsule_name(),
            used_array_capsule_name(),
            release_imported_array_callback,
        );
    }
}

unsafe extern "C" fn release_imported_schema_callback(schema: *mut ArrowSchema) {
    release_imported_schema(schema);
}

unsafe extern "C" fn release_imported_array_callback(array: *mut ArrowArray) {
    release_imported_array(array);
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

pub(crate) fn release_imported_schema(schema: *mut ArrowSchema) {
    release_imported(schema);
}

pub(crate) fn release_imported_array(array: *mut ArrowArray) {
    release_imported(array);
}

pub(crate) fn release_imported_stream(stream: *mut ArrowArrayStream) {
    release_imported(stream);
}

pub(crate) fn empty_stream_output(
    py: Python<'_>,
    schema: &ArrowSchema,
    crs: Option<&Bound<'_, PyAny>>,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    // Keystone: empty streams share the non-empty classifier — select the
    // geometry field, validate only that subtree, preserve its CRS/epoch.
    let classified = validate_empty_stream_schema(schema)?;
    let crs = crate::py::arrow::reconcile_arrow_crs(classified.crs.as_deref(), crs)?;
    let epoch = crate::py::arrow::reconcile_arrow_epoch(classified.epoch, epoch)?;
    PyGeometryArray::mixed(Vec::new(), Frame::new(crs.map(crs_arc), epoch)?).into_py_any(py)
}

pub(crate) fn schema_metadata_value(
    schema: &ArrowSchema,
    target: &[u8],
) -> PyResult<Option<Vec<u8>>> {
    if schema.metadata.is_null() {
        return Ok(None);
    }
    // SAFETY: Arrow C schema metadata is a contiguous native-endian key/value
    // blob owned by the producer while the schema release callback is live.
    unsafe {
        let mut cursor = schema.metadata.cast::<u8>();
        let pair_count = read_metadata_len(&mut cursor)?;
        for _ in 0..pair_count {
            let key_len = read_metadata_len(&mut cursor)?;
            let key = std::slice::from_raw_parts(cursor, key_len);
            cursor = cursor.add(key_len);
            let value_len = read_metadata_len(&mut cursor)?;
            let value = std::slice::from_raw_parts(cursor, value_len);
            cursor = cursor.add(value_len);
            if key == target {
                return Ok(Some(value.to_vec()));
            }
        }
    }
    Ok(None)
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

/// Resolve struct child `index` from an Arrow-C array with structural bounds
/// checks (F5): `index < n_children`, non-null `children` table, non-null child.
///
/// # Safety
/// `array` is a live producer-owned `ArrowArray`. On success the returned
/// pointer is one of `array.children[0..n_children)` and is non-null.
unsafe fn array_struct_child_ptr(array: &ArrowArray, index: usize) -> PyResult<*mut ArrowArray> {
    let n_children = usize::try_from(array.n_children).map_err(|_| {
        crate::py::errors::parse_error(
            "Arrow array n_children is negative or too large",
            crate::py::errors::ParseFormat::GeoArrow,
        )
    })?;
    if index >= n_children {
        return Err(crate::py::errors::parse_error(
            format!("Arrow struct child index {index} is out of range for n_children={n_children}"),
            crate::py::errors::ParseFormat::GeoArrow,
        ));
    }
    if array.children.is_null() {
        return Err(crate::py::errors::parse_error(
            "Arrow struct array children pointer is null",
            crate::py::errors::ParseFormat::GeoArrow,
        ));
    }
    // SAFETY: children table is non-null; index < n_children.
    let child = unsafe { *array.children.add(index) };
    if child.is_null() {
        return Err(crate::py::errors::parse_error(
            "Arrow struct array child is null",
            crate::py::errors::ParseFormat::GeoArrow,
        ));
    }
    Ok(child)
}

/// R04/D18: validate a zero-row stream batch's geometry offsets, then release
/// the array. Encoding-driven (no live schema pointer — after the first
/// non-empty batch the stream schema may already live in a capsule).
///
/// # Safety
/// `array` is a live producer-owned `ArrowArray` for this call; on return it
/// has been released. `stream_schema` is released only when validation fails.
unsafe fn validate_and_discard_zero_row_batch(
    array: &mut ArrowArray,
    stream_schema: &mut ArrowSchema,
    classified: &ClassifiedGeometrySchema,
) -> PyResult<()> {
    let geom_array = match classified.struct_child {
        Some(index) => {
            // F5: honor ArrowArray.n_children before any children.add(index).
            // Schema may select geometry child index 1 while a malformed
            // producer declares n_children=1 — OOB without this gate.
            // SAFETY: `array` is the live producer-owned batch for this call.
            match unsafe { array_struct_child_ptr(array, index) } {
                Ok(child) => {
                    // SAFETY: non-null child pointer; producer-owned for this batch.
                    unsafe { &*child }
                },
                Err(error) => {
                    release_imported_array(array);
                    release_imported_schema(stream_schema);
                    return Err(error);
                },
            }
        },
        None => &*array,
    };
    if let Err(error) = ensure_zero_row_geometry_offsets(
        geom_array,
        classified.encoding,
        classified.wkb_offset_width,
    ) {
        release_imported_array(array);
        release_imported_schema(stream_schema);
        return Err(error);
    }
    release_imported_array(array);
    Ok(())
}

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
        if get_schema(stream, &raw mut stream_schema) != 0 {
            return Err(PyTypeError::new_err(
                "Arrow C stream failed to export its schema",
            ));
        }
        // Classify once from the stream schema (field select + encoding + frame).
        let classified = match validate_empty_stream_schema(&stream_schema) {
            Ok(plan) => plan,
            Err(error) => {
                release_imported_schema(&raw mut stream_schema);
                return Err(error);
            },
        };
        // Retained non-empty batches only; fallible growth (never trust batch
        // count with infallible with_capacity / push that panics on OOM).
        let mut storages = Vec::new();
        let mut schema_capsule = None::<Py<PyAny>>;
        loop {
            let mut array = empty_array();
            if get_next(stream, &raw mut array) != 0 {
                release_imported_schema(&raw mut stream_schema);
                return Err(PyTypeError::new_err(
                    "Arrow C stream failed to export its next array",
                ));
            }
            if array.release.is_none() {
                break;
            }
            // R04: discard zero-row batches (no O(batch) retention). D18:
            // validate the empty start offset / nested list chain first.
            if array.length == 0 {
                validate_and_discard_zero_row_batch(&mut array, &mut stream_schema, &classified)?;
                continue;
            }
            if schema_capsule.is_none() {
                let schema = std::mem::replace(&mut stream_schema, empty_schema());
                schema_capsule = match owned_schema_capsule(py, schema) {
                    Ok(capsule) => Some(capsule),
                    Err(error) => {
                        release_imported_array(&raw mut array);
                        return Err(error);
                    },
                };
            }
            let array_capsule = match owned_array_capsule(py, array) {
                Ok(capsule) => capsule,
                Err(error) => {
                    return Err(error);
                },
            };
            let schema_capsule = schema_capsule
                .as_ref()
                .expect("stream schema capsule exists after non-empty batch");
            let arrow = crate::py::arrow_c::native_arrow_from_capsules_with_borrowed_schema(
                py,
                schema_capsule.bind(py),
                array_capsule.bind(py),
            )?;
            let geometry = match classified.struct_child {
                Some(index) => {
                    crate::py::arrow_c::native_arrow_struct_child(py, arrow.bind(py), index)?
                },
                None => arrow,
            };
            // Prefer the pre-classified encoding/frame so table field metadata
            // is preserved even when the native type reads root schema only.
            let storage = crate::py::arrow::arrow_storage_from_native_geometry(
                geometry.bind(py),
                classified.encoding,
                classified.wkb_offset_width,
                classified.crs.clone(),
                classified.epoch,
            )?;
            crate::try_push(&mut storages, storage)?;
        }
        if storages.is_empty() {
            let output = empty_stream_output(py, &stream_schema, crs, epoch);
            release_imported_schema(&raw mut stream_schema);
            return output.map(Some);
        }
        release_imported_schema(&raw mut stream_schema);
        crate::py::arrow::geometries_from_arrow_storages(py, storages, crs, epoch).map(Some)
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
