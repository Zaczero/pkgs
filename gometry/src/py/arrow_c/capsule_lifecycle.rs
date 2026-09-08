#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::ptr;

use crate::py::arrow_c::{
    ArrowArray, ArrowArrayStream, ArrowSchema, Bound, ExportedArray, GometryArrowArray,
    ImportedStreamGuard, Py, PyAny, PyAnyMethods as _, PyErr, PyGeometry, PyGeometryArray,
    PyResult, PyTuple, Python, StreamPrivate, array_capsule_destructor, array_capsule_name, c_char,
    ffi, release_array, release_imported_stream, release_schema, release_stream,
    schema_capsule_destructor, schema_capsule_name, stream_capsule_destructor, stream_capsule_name,
    stream_get_last_error, stream_get_next, stream_get_schema,
};

pub(crate) type ArrowReleaseCallback<T> = unsafe extern "C" fn(*mut T);
pub(crate) type CapsuleDestructor = unsafe extern "C" fn(*mut ffi::PyObject);

pub(crate) trait ArrowReleaseSlot: Sized {
    /// Return the address of the Arrow release callback slot.
    ///
    /// # Safety
    ///
    /// `ptr` must be non-null, properly aligned, and point to a live Arrow C
    /// base structure of `Self` whose fields remain accessible for the call.
    unsafe fn release_slot(ptr: *mut Self) -> *mut Option<ArrowReleaseCallback<Self>>;
}

impl ImportedStreamGuard {
    pub(crate) const fn new(stream: *mut ArrowArrayStream) -> Self {
        Self { stream }
    }
}

impl Drop for ImportedStreamGuard {
    fn drop(&mut self) {
        // SAFETY: this guard uniquely owns the stream for its lifetime; no
        // retained producer borrows remain when Drop runs.
        unsafe { release_imported_stream(self.stream) }
    }
}

impl ArrowReleaseSlot for ArrowSchema {
    unsafe fn release_slot(ptr: *mut Self) -> *mut Option<ArrowReleaseCallback<Self>> {
        // SAFETY: the caller provides a live ArrowSchema pointer.
        unsafe { &raw mut (*ptr).release }
    }
}

impl ArrowReleaseSlot for ArrowArray {
    unsafe fn release_slot(ptr: *mut Self) -> *mut Option<ArrowReleaseCallback<Self>> {
        // SAFETY: the caller provides a live ArrowArray pointer.
        unsafe { &raw mut (*ptr).release }
    }
}

impl ArrowReleaseSlot for ArrowArrayStream {
    unsafe fn release_slot(ptr: *mut Self) -> *mut Option<ArrowReleaseCallback<Self>> {
        // SAFETY: the caller provides a live ArrowArrayStream pointer.
        unsafe { &raw mut (*ptr).release }
    }
}

pub(crate) fn owned_capsule<T>(
    py: Python<'_>,
    value: Box<T>,
    name: *const c_char,
    destructor: CapsuleDestructor,
    release_on_error: ArrowReleaseCallback<T>,
) -> PyResult<Py<PyAny>> {
    let ptr = Box::into_raw(value);
    // SAFETY: `ptr` is a boxed Arrow C Data Interface shell; the capsule owns
    // the shell on success and the provided destructor releases it.
    let capsule = unsafe { ffi::PyCapsule_New(ptr.cast(), name, Some(destructor)) };
    if capsule.is_null() {
        // SAFETY: PyCapsule_New failed, so ownership never left this function.
        unsafe {
            release_on_error(ptr);
            drop(Box::from_raw(ptr));
        }
        return Err(PyErr::fetch(py));
    }
    // SAFETY: `PyCapsule_New` returns a new owned reference on success.
    Ok(unsafe { Bound::<PyAny>::from_owned_ptr(py, capsule) }.unbind())
}

/// Release and deallocate the boxed Arrow shell owned by a capsule.
///
/// # Safety
///
/// `capsule` must be a live Python capsule whose pointer under `name` or
/// `used_name` came from `Box::into_raw::<T>`. The caller must have sole
/// ownership of that shell, and no borrow of the shell or its released tree
/// may remain live.
pub(crate) unsafe fn capsule_destructor<T>(
    capsule: *mut ffi::PyObject,
    name: *const c_char,
    used_name: *const c_char,
    release: ArrowReleaseCallback<T>,
) {
    // SAFETY: callers pass the exact capsule names for `T`; moved capsules may
    // use the `used_*` name but still leave the shell box owned here.
    unsafe {
        let ptr = capsule_destructor_pointer::<T>(capsule, name, used_name);
        if ptr.is_null() {
            return;
        }
        release(ptr);
        drop(Box::from_raw(ptr));
    }
}

/// Release an imported Arrow C base structure through its producer callback.
///
/// # Safety
///
/// - `ptr` is null **or** a live producer-owned base structure whose release
///   slot is still set (or already cleared — then this is a no-op).
/// - No safe borrow of `*ptr` (or any field/table it owns) remains live across
///   this call. Release-while-borrowed is undefined behaviour.
/// - The caller does not release the same live structure twice concurrently.
pub(crate) unsafe fn release_imported<T: ArrowReleaseSlot>(ptr: *mut T) {
    // SAFETY: caller guarantees no live borrows and a valid (or null) pointer.
    unsafe {
        if !ptr.is_null()
            && let Some(release) = *T::release_slot(ptr)
        {
            release(ptr);
        }
    }
}

/// Recover the Arrow shell pointer owned by a named or consumed capsule.
///
/// # Safety
///
/// `capsule` must be null or a live Python object that may safely be passed to
/// the CPython capsule API. A non-null returned pointer remains owned by the
/// capsule and may only be consumed by the capsule's destructor.
pub(crate) unsafe fn capsule_destructor_pointer<T>(
    capsule: *mut ffi::PyObject,
    name: *const c_char,
    used_name: *const c_char,
) -> *mut T {
    // SAFETY: `PyCapsule_IsValid` checks the name without setting an exception.
    // Consumers may rename a moved capsule to `used_arrow_*`; we still own and
    // drop the boxed struct shell, but its release slot should already be NULL.
    unsafe {
        if ffi::PyCapsule_IsValid(capsule, name) != 0 {
            return ffi::PyCapsule_GetPointer(capsule, name).cast();
        }
        if ffi::PyCapsule_IsValid(capsule, used_name) != 0 {
            return ffi::PyCapsule_GetPointer(capsule, used_name).cast();
        }
    }
    ptr::null_mut()
}
pub(crate) fn register_loaded_pyarrow_extension_types(py: Python<'_>) -> PyResult<()> {
    crate::py::arrow::gometry_arrow_module(py)?
        .call_method0("_register_extension_types_if_available")?;
    Ok(())
}

pub(crate) fn geometry_to_schema_capsule(
    py: Python<'_>,
    geometry: &PyGeometry,
) -> PyResult<Py<PyAny>> {
    register_loaded_pyarrow_extension_types(py)?;
    let shapes = vec![geometry.shape.shape().clone()];
    let crs = geometry.crs_str().map(str::to_owned);
    let epoch = geometry.epoch();
    let plan = py
        .detach(move || {
            let plan = crate::py::arrow_c::plan_shapes(&shapes, crs, epoch)?;
            crate::py::arrow_c::prepare_export_plan(plan)
        })
        .map_err(|error| error.into_py_err(py))?;
    let schema = crate::py::arrow_c::materialize_schema_plan(plan)?;
    schema_capsule(py, schema)
}

pub(crate) fn array_to_schema_capsule(
    py: Python<'_>,
    array: &PyGeometryArray,
) -> PyResult<Py<PyAny>> {
    register_loaded_pyarrow_extension_types(py)?;
    // Array frame is authoritative (per-row frames no longer exist).
    let array = array.clone();
    let plan = py
        .detach(move || {
            let plan = crate::py::arrow_c::plan_geometry_array(
                &array,
                crate::py::arrow::ArrowEncoding::Auto,
            )?;
            crate::py::arrow_c::prepare_export_plan(plan)
        })
        .map_err(|error| error.into_py_err(py))?;
    let schema = crate::py::arrow_c::materialize_schema_plan(plan)?;
    schema_capsule(py, schema)
}

pub(crate) fn geometry_to_array_capsules(
    py: Python<'_>,
    geometry: &PyGeometry,
) -> PyResult<Py<PyAny>> {
    register_loaded_pyarrow_extension_types(py)?;
    let shapes = vec![geometry.shape.shape().clone()];
    let crs = geometry.crs_str().map(str::to_owned);
    let epoch = geometry.epoch();
    let plan = py
        .detach(move || {
            let plan = crate::py::arrow_c::plan_shapes(&shapes, crs, epoch)?;
            crate::py::arrow_c::prepare_export_plan(plan)
        })
        .map_err(|error| error.into_py_err(py))?;
    let export = crate::py::arrow_c::materialize_export_plan(plan)?;
    array_capsules(py, export)
}

pub(crate) fn array_to_array_capsules(
    py: Python<'_>,
    array: &PyGeometryArray,
) -> PyResult<Py<PyAny>> {
    register_loaded_pyarrow_extension_types(py)?;
    // Storage-direct: retain coordinate/CSR Arcs; no per-row PyGeometry.
    let array = array.clone();
    let plan = py
        .detach(move || {
            let plan = crate::py::arrow_c::plan_geometry_array(
                &array,
                crate::py::arrow::ArrowEncoding::Auto,
            )?;
            crate::py::arrow_c::prepare_export_plan(plan)
        })
        .map_err(|error| error.into_py_err(py))?;
    let export = crate::py::arrow_c::materialize_export_plan(plan)?;
    array_capsules(py, export)
}

pub(crate) fn geometry_to_stream_capsule(
    py: Python<'_>,
    geometry: &PyGeometry,
) -> PyResult<Py<PyAny>> {
    register_loaded_pyarrow_extension_types(py)?;
    let shapes = vec![geometry.shape.shape().clone()];
    let crs = geometry.crs_str().map(str::to_owned);
    let epoch = geometry.epoch();
    let plan = py
        .detach(move || {
            let plan = crate::py::arrow_c::plan_shapes(&shapes, crs, epoch)?;
            crate::py::arrow_c::prepare_export_plan(plan)
        })
        .map_err(|error| error.into_py_err(py))?;
    let export = crate::py::arrow_c::materialize_export_plan(plan)?;
    stream_capsule(py, export)
}

pub(crate) fn array_to_stream_capsule(
    py: Python<'_>,
    array: &PyGeometryArray,
) -> PyResult<Py<PyAny>> {
    register_loaded_pyarrow_extension_types(py)?;
    let array = array.clone();
    let plan = py
        .detach(move || {
            let plan = crate::py::arrow_c::plan_geometry_array(
                &array,
                crate::py::arrow::ArrowEncoding::Auto,
            )?;
            crate::py::arrow_c::prepare_export_plan(plan)
        })
        .map_err(|error| error.into_py_err(py))?;
    let export = crate::py::arrow_c::materialize_export_plan(plan)?;
    stream_capsule(py, export)
}

pub(crate) fn schema_capsule(
    py: Python<'_>,
    schema: crate::py::arrow_c::GometryArrowSchema,
) -> PyResult<Py<PyAny>> {
    owned_capsule(
        py,
        schema.into_box(),
        schema_capsule_name(),
        schema_capsule_destructor,
        release_schema,
    )
}

pub(crate) fn array_capsules(py: Python<'_>, export: ExportedArray) -> PyResult<Py<PyAny>> {
    let schema = schema_capsule(py, export.schema)?;
    let array = array_capsule(py, export.array)?;
    Ok(PyTuple::new(py, [schema, array])?.unbind().into_any())
}

/// Import one native export into PyArrow after the native plan has completed.
/// Capsule creation and consumption stay attached because both sides invoke
/// Python-owned release callbacks; no native encoding work happens here.
pub(crate) fn pyarrow_array_from_export(
    py: Python<'_>,
    export: ExportedArray,
) -> PyResult<Py<PyAny>> {
    // PyArrow must be loaded before the existing registry hook runs. This is
    // the attached materialization boundary; direct capsule exports remain
    // dependency-free.
    let pyarrow = crate::py::arrow::pyarrow_module(py)?;
    register_loaded_pyarrow_extension_types(py)?;
    let capsules = array_capsules(py, export)?;
    let tuple = capsules.bind(py).cast::<PyTuple>()?;
    pyarrow
        .getattr(pyo3::intern!(py, "Array"))?
        .call_method1(
            pyo3::intern!(py, "_import_from_c_capsule"),
            (tuple.get_item(0)?, tuple.get_item(1)?),
        )
        .map(Bound::unbind)
}

pub(crate) fn array_capsule(py: Python<'_>, array: GometryArrowArray) -> PyResult<Py<PyAny>> {
    owned_capsule(
        py,
        array.into_box(),
        array_capsule_name(),
        array_capsule_destructor,
        release_array,
    )
}

pub(crate) fn stream_from_export(export: ExportedArray) -> PyResult<Box<ArrowArrayStream>> {
    let private = Box::try_new(StreamPrivate {
        schema: export.schema_node,
        array: Some(export.array),
        last_error: c"",
    })
    .map_err(crate::error::Error::from)?;
    let mut stream = Box::try_new(ArrowArrayStream {
        get_schema: Some(stream_get_schema),
        get_next: Some(stream_get_next),
        get_last_error: Some(stream_get_last_error),
        release: Some(release_stream),
        private_data: std::ptr::null_mut(),
    })
    .map_err(crate::error::Error::from)?;
    stream.private_data = Box::into_raw(private).cast();
    Ok(stream)
}

pub(crate) fn stream_capsule(py: Python<'_>, export: ExportedArray) -> PyResult<Py<PyAny>> {
    let stream = stream_from_export(export)?;
    owned_capsule(
        py,
        stream,
        stream_capsule_name(),
        stream_capsule_destructor,
        release_stream,
    )
}
