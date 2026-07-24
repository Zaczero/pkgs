#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::ptr;

use crate::py::arrow_c::*;

trait ArrowReleaseTree: ArrowReleaseSlot {
    type Private;

    unsafe fn private_data_slot(ptr: *mut Self) -> *mut *mut c_void;
    unsafe fn release_private(private: *mut Self::Private);
}

unsafe fn release_tree<T: ArrowReleaseTree>(value: *mut T) {
    if value.is_null() {
        return;
    }
    // SAFETY: C Data Interface release callbacks receive a valid exported
    // shell pointer. The release slot is the Arrow-mandated idempotence guard;
    // the private box owns every child/buffer holder referenced by the shell.
    unsafe {
        let release_slot = T::release_slot(value);
        if (*release_slot).is_none() {
            return;
        }
        let private_slot = T::private_data_slot(value);
        let private = (*private_slot).cast::<T::Private>();
        if !private.is_null() {
            T::release_private(private);
        }
        *release_slot = None;
        *private_slot = ptr::null_mut();
    }
}

impl ArrowReleaseTree for ArrowSchema {
    type Private = SchemaPrivate;

    unsafe fn private_data_slot(ptr: *mut Self) -> *mut *mut c_void {
        // SAFETY: the caller provides a live ArrowSchema pointer.
        unsafe { &raw mut (*ptr).private_data }
    }

    unsafe fn release_private(private: *mut Self::Private) {
        // SAFETY: `SchemaPrivate` owns the children vector; children are
        // released before the private box drops their shells.
        unsafe {
            for child in &mut (*private).children {
                release_schema(child.as_mut());
            }
            drop(Box::from_raw(private));
        }
    }
}

impl ArrowReleaseTree for ArrowArray {
    type Private = ArrayPrivate;

    unsafe fn private_data_slot(ptr: *mut Self) -> *mut *mut c_void {
        // SAFETY: the caller provides a live ArrowArray pointer.
        unsafe { &raw mut (*ptr).private_data }
    }

    unsafe fn release_private(private: *mut Self::Private) {
        // SAFETY: `ArrayPrivate` owns child shells and buffer-holder Arcs;
        // children are released before the private box drops their shells.
        unsafe {
            for child in &mut (*private).children {
                release_array(child.as_mut());
            }
            drop(Box::from_raw(private));
        }
    }
}

impl ArrowReleaseTree for ArrowArrayStream {
    type Private = StreamPrivate;

    unsafe fn private_data_slot(ptr: *mut Self) -> *mut *mut c_void {
        // SAFETY: the caller provides a live ArrowArrayStream pointer.
        unsafe { &raw mut (*ptr).private_data }
    }

    unsafe fn release_private(private: *mut Self::Private) {
        // SAFETY: the stream private owns the not-yet-yielded one-shot array,
        // if any. Once `get_next` moves it into the consumer's out pointer,
        // ownership leaves the stream and this option is `None`.
        unsafe {
            let mut private = Box::from_raw(private);
            if let Some(mut array) = private.array.take() {
                release_array(array.as_mut());
            }
            drop(private);
        }
    }
}

pub(crate) unsafe extern "C" fn release_schema(schema: *mut ArrowSchema) {
    // SAFETY: `release_tree` implements the shared Arrow release-slot/private
    // box order for exported schemas.
    unsafe { release_tree(schema) }
}

pub(crate) unsafe extern "C" fn release_array(array: *mut ArrowArray) {
    // SAFETY: `release_tree` implements the shared Arrow release-slot/private
    // box order for exported arrays.
    unsafe { release_tree(array) }
}

pub(crate) unsafe extern "C" fn release_stream(stream: *mut ArrowArrayStream) {
    // SAFETY: `release_tree` implements the shared Arrow release-slot/private
    // box order for exported streams.
    unsafe { release_tree(stream) }
}

pub(crate) unsafe extern "C" fn stream_get_schema(
    stream: *mut ArrowArrayStream,
    out: *mut ArrowSchema,
) -> c_int {
    if stream.is_null() || out.is_null() {
        return -1;
    }
    // SAFETY: `private_data` is a `StreamPrivate` while release is present.
    // `SchemaNode::into_schema` allocates an independent schema tree for the
    // caller-owned out slot.
    unsafe {
        let private = (*stream).private_data.cast::<StreamPrivate>();
        if private.is_null() {
            return -1;
        }
        ptr::write(out, *(*private).schema.clone().into_schema());
    }
    0
}

pub(crate) unsafe extern "C" fn stream_get_next(
    stream: *mut ArrowArrayStream,
    out: *mut ArrowArray,
) -> c_int {
    if stream.is_null() || out.is_null() {
        return -1;
    }
    // SAFETY: the single prebuilt array is moved exactly once into `out`.
    // Exhaustion is represented by the Arrow convention `release = NULL`.
    unsafe {
        let private = (*stream).private_data.cast::<StreamPrivate>();
        if private.is_null() {
            return -1;
        }
        if let Some(array) = (*private).array.take() {
            ptr::write(out, *array);
        } else {
            ptr::write(out, empty_array());
        }
    }
    0
}

pub(crate) unsafe extern "C" fn stream_get_last_error(
    stream: *mut ArrowArrayStream,
) -> *const c_char {
    if stream.is_null() {
        return ptr::null();
    }
    // SAFETY: `last_error` is owned by `StreamPrivate` and lives until stream
    // release.
    unsafe {
        let private = (*stream).private_data.cast::<StreamPrivate>();
        if private.is_null() {
            return ptr::null();
        }
        (*private).last_error.as_ptr()
    }
}

pub(crate) unsafe extern "C" fn schema_capsule_destructor(capsule: *mut ffi::PyObject) {
    // SAFETY: the capsule was created with this exact name and owns a boxed
    // `ArrowSchema`. If a consumer already released it, `release` is `None`.
    unsafe {
        capsule_destructor::<ArrowSchema>(
            capsule,
            schema_capsule_name(),
            used_schema_capsule_name(),
            release_schema,
        );
    }
}

pub(crate) unsafe extern "C" fn array_capsule_destructor(capsule: *mut ffi::PyObject) {
    // SAFETY: same ownership shape as `schema_capsule_destructor`, for
    // `ArrowArray`.
    unsafe {
        capsule_destructor::<ArrowArray>(
            capsule,
            array_capsule_name(),
            used_array_capsule_name(),
            release_array,
        );
    }
}

pub(crate) unsafe extern "C" fn stream_capsule_destructor(capsule: *mut ffi::PyObject) {
    // SAFETY: same ownership shape as `schema_capsule_destructor`, for
    // `ArrowArrayStream`.
    unsafe {
        capsule_destructor::<ArrowArrayStream>(
            capsule,
            stream_capsule_name(),
            used_stream_capsule_name(),
            release_stream,
        );
    }
}
